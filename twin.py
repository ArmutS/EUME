r"""
Calistirma:
    .\.venv312\Scripts\python.exe -m uvicorn digital_twin_server:app --reload

Arayuz:
    http://127.0.0.1:8000

WebSocket:
    ws://127.0.0.1:8000/ws
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import simpy
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from anodizing_des import (
    AnodizingPlant,
    ExcelDataLoader,
    default_analysis_workbook,
    default_output_workbook,
    default_source_workbook,
    format_seconds_as_hms,
    route_family_label,
)


UI_DIR = Path(__file__).parent / "ui"
BASE_PLAYBACK_RATE_SIM_SECONDS = 300.0
MAX_REAL_DELAY_SECONDS = 1.25
JOB_COLORS = {
    "parlak": "#5ec2ff",
    "sakem": "#8cb4ff",
    "pan": "#59d5b0",
    "natural": "#f3d36b",
    "man": "#ff9f68",
    "asitmat": "#ff6b7d",
}


class StartSimulationRequest(BaseModel):
    use_total_bars: bool = Field(default=True)
    total_bars: int | None = Field(default=20, ge=1, le=5000)
    use_duration_hours: bool = Field(default=False)
    duration_hours: float = Field(default=8.0, ge=0.0, le=168.0)
    replication_count: int = Field(default=1, ge=1, le=100)
    use_havuz_sequence: bool = Field(default=False)
    use_havuz_actual: bool = Field(default=False)
    use_constant_durations: bool = Field(default=True)
    use_q_arrival_mix: bool = Field(default=False)
    speed_multiplier: float = Field(default=1.0, ge=1.0, le=10.0)
    seed: int = Field(default=42, ge=0, le=2_147_483_647)


@dataclass(slots=True)
class PreparedRun:
    scene: dict[str, Any]
    timeline: list[dict[str, Any]]
    summary: dict[str, Any]
    plant: AnodizingPlant


@lru_cache(maxsize=4)
def load_default_data(
    use_constant_durations: bool = True,
    use_q_arrival_mix: bool = False,
) -> Any:
    loader = ExcelDataLoader(
        source_workbook=default_source_workbook(),
        analysis_workbook=default_analysis_workbook(),
        timeout_csv=None,
        allow_missing_timeouts=True,
        use_constant_durations=use_constant_durations,
        use_q_arrival_mix=use_q_arrival_mix,
    )
    return loader.load()


def build_initial_scene_payload(data: Any, seed: int) -> dict[str, Any]:
    env = simpy.Environment()
    plant = AnodizingPlant(env, data, seed=seed)
    return build_scene_payload(data.station_definitions, data.station_positions, plant)


def export_report(prepared: PreparedRun, request: StartSimulationRequest, data: Any) -> str:
    replication_results = [
        {
            "replication": 1,
            "seed": request.seed,
            "summary": prepared.summary,
            "wip_trace": prepared.plant.build_wip_trace(),
        }
    ]
    for replication_index in range(2, int(request.replication_count) + 1):
        replica_request = request.model_copy(update={"seed": request.seed + replication_index - 1})
        replica_prepared = run_simulation_capture(replica_request, data)
        replication_results.append(
            {
                "replication": replication_index,
                "seed": replica_request.seed,
                "summary": replica_prepared.summary,
                "wip_trace": replica_prepared.plant.build_wip_trace(),
            }
        )
    return str(
        prepared.plant.export_excel_report(
            default_output_workbook(),
            replication_results=replication_results,
        )
    )


def run_simulation_capture(request: StartSimulationRequest, data: Any | None = None) -> PreparedRun:
    if not request.use_total_bars and not request.use_duration_hours:
        if not request.use_havuz_sequence and not request.use_havuz_actual:
            raise ValueError("En az bir kosul secilmeli: toplam bara veya simulasyon suresi.")

    if data is None:
        data = load_default_data(request.use_constant_durations, request.use_q_arrival_mix)

    env = simpy.Environment()
    plant = AnodizingPlant(env, data, seed=request.seed)
    arrival_limit = request.total_bars if request.use_total_bars else None
    if request.use_havuz_actual:
        env.process(
            plant.replay_arrival_generator(
                data.havuz_replay_records,
                arrival_limit=arrival_limit,
                use_fixed_step_seconds=True,
            )
        )
    elif request.use_havuz_sequence:
        env.process(
            plant.replay_arrival_generator(
                data.havuz_replay_records,
                arrival_limit=arrival_limit,
                use_fixed_step_seconds=False,
            )
        )
    else:
        env.process(plant.arrival_generator(arrival_limit=arrival_limit))

    until_seconds = float(request.duration_hours) * 3600.0 if request.use_duration_hours else 0.0
    if request.use_duration_hours and until_seconds > 0:
        env.run(until=until_seconds)
    else:
        env.run(until=plant.all_done)

    summary = plant.summary()
    scene = build_scene_payload(data.station_definitions, data.station_positions, plant)
    timeline = build_timeline_payload(plant)
    return PreparedRun(
        scene=scene,
        timeline=timeline,
        summary=summary,
        plant=plant,
    )


def build_scene_payload(station_definitions, station_positions, plant: AnodizingPlant) -> dict[str, Any]:
    stations = []
    ordered = sorted(
        station_definitions.values(),
        key=lambda definition: (definition.x_m, definition.display_name),
    )
    for definition in ordered:
        if definition.station_id.startswith("buffer_"):
            continue
        stations.append(
            {
                "id": definition.station_id,
                "label": definition.display_name,
                "x": float(definition.x_m),
                "kind": (
                    "sink"
                    if definition.is_sink
                    else "virtual"
                    if definition.is_virtual
                    else "process"
                ),
                "processGroup": definition.process_group,
            }
        )

    cranes = []
    for crane_id, crane in plant.cranes.items():
        zone_min, zone_max = plant.rail_controller.zone_bounds[crane_id]
        cranes.append(
            {
                "id": crane_id,
                "label": crane_id.replace("vinc", "Vinç "),
                "x": float(crane.home_x),
                "homeX": float(crane.home_x),
                "zoneMinX": float(zone_min),
                "zoneMaxX": float(zone_max),
            }
        )

    return {
        "bounds": {
            "minX": float(min(station["x"] for station in stations)),
            "maxX": float(max(station["x"] for station in stations)),
        },
        "stations": stations,
        "cranes": cranes,
        "dripHoldX": float(plant.drip_hold_x_m),
    }


def ensure_batch(batches: dict[float, dict[str, Any]], timestamp: float) -> dict[str, Any]:
    batch = batches.get(timestamp)
    if batch is None:
        batch = {"time": timestamp, "timeHms": format_seconds_as_hms(timestamp), "ops": [], "logs": [], "stats": None}
        batches[timestamp] = batch
    return batch


def add_op(batches: dict[float, dict[str, Any]], timestamp: float, op: dict[str, Any]) -> None:
    ensure_batch(batches, timestamp)["ops"].append(op)


def add_log(batches: dict[float, dict[str, Any]], row: dict[str, Any]) -> None:
    ensure_batch(batches, float(row["time_seconds"]))["logs"].append(
        {
            "time": float(row["time_seconds"]),
            "timeHms": row["time_hms"],
            "eventType": row["event_type"],
            "message": row["message"],
            "jobId": row.get("bara_no"),
            "craneId": row.get("crane_id"),
        }
    )


def add_stats(batches: dict[float, dict[str, Any]], timestamp: float, stats: dict[str, int]) -> None:
    ensure_batch(batches, timestamp)["stats"] = dict(stats)


def build_timeline_payload(plant: AnodizingPlant) -> list[dict[str, Any]]:
    event_rows = [
        row
        for _, row in sorted(
            enumerate(plant.event_log),
            key=lambda item: (float(item[1]["time_seconds"]), item[0]),
        )
    ]
    station_x = {station_id: definition.x_m for station_id, definition in plant.station_definitions.items()}
    batches: dict[float, dict[str, Any]] = {}
    pending_pickups: dict[str, dict[str, Any]] = {}
    held_jobs: dict[str, dict[str, Any]] = {}
    stats = {"arrivals": 0, "completed": 0, "wip": 0, "qualityViolations": 0}

    for row in event_rows:
        timestamp = float(row["time_seconds"])
        event_type = str(row["event_type"])
        job_id = int(row["bara_no"]) if row.get("bara_no") is not None else None
        crane_id = row.get("crane_id")
        source_station_id = row.get("source_station_id")
        destination_station_id = row.get("destination_station_id")

        add_log(batches, row)

        if event_type == "arrival" and job_id is not None:
            stats["arrivals"] += 1
            stats["wip"] += 1
            add_stats(batches, timestamp, stats)
            add_op(
                batches,
                timestamp,
                {
                    "kind": "create_job",
                    "jobId": job_id,
                    "routeFamily": row.get("route_family"),
                    "label": row.get("cesit"),
                    "micron": row.get("mikron"),
                    "color": JOB_COLORS.get(str(row.get("route_family")), "#8cb4ff"),
                    "stationId": source_station_id or "entry_conveyor",
                },
            )
            continue

        if event_type in {"pickup", "rinse_lift"} and crane_id and job_id is not None:
            pending_pickups[crane_id] = {
                "jobId": job_id,
                "time": timestamp,
                "sourceStationId": source_station_id,
                "fromX": float(station_x.get(source_station_id or "", 0.0)),
            }
            add_op(
                batches,
                timestamp,
                {
                    "kind": "attach_job",
                    "jobId": job_id,
                    "craneId": crane_id,
                    "sourceStationId": source_station_id,
                },
            )
            continue

        if event_type == "drip_start" and crane_id and job_id is not None:
            pending = pending_pickups.pop(crane_id, None)
            if pending is not None:
                add_op(
                    batches,
                    float(pending["time"]),
                    {
                        "kind": "move_crane",
                        "craneId": crane_id,
                        "jobId": pending["jobId"],
                        "fromX": pending["fromX"],
                        "toX": float(plant.drip_hold_x_m),
                        "durationSeconds": max(0.0, timestamp - float(pending["time"])),
                    },
                )
            hold_seconds = float(row.get("sure_sn") or 0.0)
            add_op(
                batches,
                timestamp,
                {
                    "kind": "hold_job",
                    "craneId": crane_id,
                    "jobId": job_id,
                    "holdX": float(plant.drip_hold_x_m),
                    "durationSeconds": hold_seconds,
                },
            )
            held_jobs[crane_id] = {
                "jobId": job_id,
                "startTime": timestamp + hold_seconds,
                "fromX": float(plant.drip_hold_x_m),
            }
            continue

        if event_type in {"drop", "aux_buffer_drop"} and crane_id and job_id is not None:
            if crane_id in held_jobs:
                held = held_jobs.pop(crane_id)
                add_op(
                    batches,
                    float(held["startTime"]),
                    {
                        "kind": "move_crane",
                        "craneId": crane_id,
                        "jobId": held["jobId"],
                        "fromX": float(held["fromX"]),
                        "toX": float(station_x.get(destination_station_id or "", held["fromX"])),
                        "durationSeconds": max(0.0, timestamp - float(held["startTime"])),
                    },
                )
            else:
                pending = pending_pickups.pop(crane_id, None)
                if pending is not None:
                    add_op(
                        batches,
                        float(pending["time"]),
                        {
                            "kind": "move_crane",
                            "craneId": crane_id,
                            "jobId": pending["jobId"],
                            "fromX": float(pending["fromX"]),
                            "toX": float(station_x.get(destination_station_id or "", pending["fromX"])),
                            "durationSeconds": max(0.0, timestamp - float(pending["time"])),
                        },
                    )

            add_op(
                batches,
                timestamp,
                {
                    "kind": "place_job",
                    "jobId": job_id,
                    "craneId": crane_id,
                    "stationId": destination_station_id,
                    "stationLabel": row.get("hedef"),
                },
            )
            continue

        if event_type == "departure" and job_id is not None:
            stats["completed"] += 1
            stats["wip"] = max(0, stats["wip"] - 1)
            add_stats(batches, timestamp, stats)
            add_op(
                batches,
                timestamp,
                {
                    "kind": "complete_job",
                    "jobId": job_id,
                },
            )
            continue

        if event_type == "flex_claim" and crane_id and job_id is not None:
            add_op(
                batches,
                timestamp,
                {
                    "kind": "pulse_crane",
                    "craneId": crane_id,
                    "jobId": job_id,
                    "durationSeconds": 1.4,
                },
            )
            continue

    batches[0.0] = {
        "time": 0.0,
        "timeHms": "00:00:00",
        "ops": [],
        "logs": [],
        "stats": {"arrivals": 0, "completed": 0, "wip": 0, "qualityViolations": 0},
    } | batches.get(0.0, {})

    return [batches[key] for key in sorted(batches)]


class DigitalTwinRuntime:
    def __init__(self) -> None:
        self.connections: set[WebSocket] = set()
        self.playback_task: asyncio.Task | None = None
        self.generation = 0
        self.state = "idle"
        self.latest_scene: dict[str, Any] | None = None
        self.latest_stats: dict[str, Any] = {
            "arrivals": 0,
            "completed": 0,
            "wip": 0,
            "qualityViolations": 0,
        }
        self.latest_summary: dict[str, Any] | None = None
        self.report_path: str | None = None
        self.status_message = "Hazir"

    async def register(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.connections.add(websocket)
        await websocket.send_json(
            {
                "type": "hello",
                "status": self.state,
                "message": self.status_message,
                "stats": self.latest_stats,
                "summary": self.latest_summary,
                "reportPath": self.report_path,
                "reportUrl": "/api/report/latest" if self.report_path else None,
            }
        )
        if self.latest_scene is not None:
            await websocket.send_json({"type": "scene_init", "scene": self.latest_scene})

    def unregister(self, websocket: WebSocket) -> None:
        self.connections.discard(websocket)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        stale: list[WebSocket] = []
        for websocket in self.connections:
            try:
                await websocket.send_json(payload)
            except Exception:
                stale.append(websocket)
        for websocket in stale:
            self.unregister(websocket)

    async def start(self, request: StartSimulationRequest) -> None:
        await self.reset(announce=False)
        self.generation += 1
        generation = self.generation
        self.state = "preparing"
        self.status_message = "Simulasyon hazirlaniyor"
        await self.broadcast({"type": "run_state", "status": self.state, "message": self.status_message})
        self.playback_task = asyncio.create_task(self._prepare_and_play(generation, request))

    async def reset(self, announce: bool = True) -> None:
        self.generation += 1
        if self.playback_task is not None:
            self.playback_task.cancel()
            try:
                await self.playback_task
            except asyncio.CancelledError:
                pass
            self.playback_task = None
        self.state = "idle"
        self.status_message = "Hazir"
        self.latest_scene = None
        self.latest_summary = None
        self.report_path = None
        self.latest_stats = {"arrivals": 0, "completed": 0, "wip": 0, "qualityViolations": 0}
        if announce:
            await self.broadcast({"type": "reset"})
            await self.broadcast({"type": "run_state", "status": self.state, "message": self.status_message})

    async def _prepare_and_play(self, generation: int, request: StartSimulationRequest) -> None:
        try:
            data = await asyncio.to_thread(
                load_default_data,
                request.use_constant_durations,
                request.use_q_arrival_mix,
            )
            if generation != self.generation:
                return

            initial_scene = await asyncio.to_thread(build_initial_scene_payload, data, request.seed)
            if generation != self.generation:
                return

            self.latest_scene = initial_scene
            await self.broadcast({"type": "reset"})
            await self.broadcast({"type": "scene_init", "scene": initial_scene})
            await self.broadcast(
                {
                    "type": "run_state",
                    "status": self.state,
                    "message": "Sahne hazir, simulasyon hesaplaniyor",
                }
            )

            prepared = await asyncio.to_thread(run_simulation_capture, request, data)
            if generation != self.generation:
                return

            self.latest_scene = prepared.scene
            self.latest_summary = prepared.summary
            self.state = "playing"
            self.status_message = "Simulasyon oynatiliyor"
            await self.broadcast({"type": "run_state", "status": self.state, "message": self.status_message})
            report_task = asyncio.create_task(asyncio.to_thread(export_report, prepared, request, data))

            previous_time = 0.0
            for batch in prepared.timeline:
                if generation != self.generation:
                    return
                current_time = float(batch["time"])
                sim_delta = max(0.0, current_time - previous_time)
                delay = min(
                    sim_delta / (BASE_PLAYBACK_RATE_SIM_SECONDS * float(request.speed_multiplier)),
                    MAX_REAL_DELAY_SECONDS,
                )
                if delay > 0:
                    await asyncio.sleep(delay)
                previous_time = current_time

                if batch.get("stats") is not None:
                    self.latest_stats = dict(batch["stats"])
                    await self.broadcast(
                        {
                            "type": "stats",
                            "simTime": current_time,
                            "timeHms": batch["timeHms"],
                            "stats": self.latest_stats,
                        }
                    )
                if batch.get("logs"):
                    await self.broadcast(
                        {
                            "type": "log_batch",
                            "simTime": current_time,
                            "timeHms": batch["timeHms"],
                            "entries": batch["logs"],
                        }
                    )
                if batch.get("ops"):
                    await self.broadcast(
                        {
                            "type": "timeline_batch",
                            "simTime": current_time,
                            "timeHms": batch["timeHms"],
                            "ops": batch["ops"],
                        }
                    )

            report_path = None
            try:
                report_path = await report_task
            except Exception:
                report_path = None
            if generation != self.generation:
                return

            self.report_path = report_path
            self.state = "completed"
            self.status_message = "Simulasyon tamamlandi"
            await self.broadcast(
                {
                    "type": "run_complete",
                    "status": self.state,
                    "message": self.status_message,
                    "summary": prepared.summary,
                    "reportPath": report_path,
                    "reportUrl": "/api/report/latest" if report_path else None,
                }
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.state = "error"
            self.status_message = str(exc)
            await self.broadcast({"type": "run_state", "status": "error", "message": str(exc)})


runtime = DigitalTwinRuntime()
app = FastAPI(title="Anodizing Digital Twin")
app.mount("/ui", StaticFiles(directory=UI_DIR), name="ui")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(UI_DIR / "index.html")


@app.get("/api/status")
async def api_status() -> dict[str, Any]:
    return {
        "status": runtime.state,
        "message": runtime.status_message,
        "stats": runtime.latest_stats,
        "summary": runtime.latest_summary,
        "reportPath": runtime.report_path,
        "reportUrl": "/api/report/latest" if runtime.report_path else None,
        "scene": runtime.latest_scene,
    }


@app.post("/api/start")
async def api_start(request: StartSimulationRequest) -> dict[str, Any]:
    await runtime.start(request)
    return {"ok": True, "status": runtime.state}


@app.post("/api/reset")
async def api_reset() -> dict[str, Any]:
    await runtime.reset()
    return {"ok": True, "status": runtime.state}


@app.get("/api/report/latest")
async def api_report_latest() -> FileResponse:
    if not runtime.report_path:
        raise HTTPException(status_code=404, detail="Henuz rapor yok.")
    return FileResponse(runtime.report_path, filename=Path(runtime.report_path).name)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await runtime.register(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        runtime.unregister(websocket)
