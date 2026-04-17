import pandas as pd
import warnings

warnings.filterwarnings("ignore")

file_path = "veriseti.xlsx"

print("1. Excel sayfaları okunuyor...")
df_polisaj = pd.read_excel(file_path, sheet_name="Polisaj Üretim")
df_uretim = pd.read_excel(file_path, sheet_name="Eloksal Üretim")
df_tespit = pd.read_excel(file_path, sheet_name="Tespit Duruş Süre")
df_kalite = pd.read_excel(file_path, sheet_name="Eloksal Kalite Kontrol Takip")
df_havuz = pd.read_excel(file_path, sheet_name="Havuz Süreler")


def sutun_standartlastir(df):
    df.columns = df.columns.str.strip()
    sozluk = {
        "BaraNo.": "Bara No.",
        "Bara No": "Bara No.",
        "ProfilNo.": "Profil No.",
        "Profil No": "Profil No.",
    }
    df.rename(columns=sozluk, inplace=True)
    return df


print("2. Başlık kaosu standartlaştırılıyor...")
df_polisaj = sutun_standartlastir(df_polisaj)
df_uretim = sutun_standartlastir(df_uretim)
df_tespit = sutun_standartlastir(df_tespit)
df_kalite = sutun_standartlastir(df_kalite)
df_havuz = sutun_standartlastir(df_havuz)


def veri_temizle(df):
    if "Tarih" in df.columns:
        df["Tarih"] = pd.to_datetime(df["Tarih"], errors="coerce").dt.normalize()

    for col in ["Bara No.", "Profil No."]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
            )
            df[col] = df[col].replace("nan", pd.NA)
    return df


print("3. Veriler temizleniyor...")
df_polisaj = veri_temizle(df_polisaj)
df_uretim = veri_temizle(df_uretim)
df_tespit = veri_temizle(df_tespit)
df_kalite = veri_temizle(df_kalite)
df_havuz = veri_temizle(df_havuz)

print("4. Kümeler birbirine kaynaklanıyor...")
ortak_anahtar = ["Tarih", "Bara No.", "Profil No."]


vs1 = pd.merge(
    df_uretim, df_kalite, on=ortak_anahtar, how="inner", suffixes=("_Uret", "_Kalite")
)


vs2 = vs1.copy()
vs2 = pd.merge(vs2, df_polisaj, on=["Tarih", "Profil No."], how="inner")
vs2 = pd.merge(vs2, df_tespit, on=["Tarih", "Bara No."], how="inner")


vs3 = pd.merge(vs1, df_havuz, on=ortak_anahtar, how="inner")


vs4 = pd.merge(
    df_uretim, df_havuz, on=ortak_anahtar, how="inner", suffixes=("_Uret", "_Havuz")
)

print("5. Mükerrer kayıtlar silinip Excel'e aktarılıyor...")
vs1 = vs1.drop_duplicates(subset=ortak_anahtar)
vs2 = vs2.drop_duplicates(subset=ortak_anahtar)
vs3 = vs3.drop_duplicates(subset=ortak_anahtar)
vs4 = vs4.drop_duplicates(subset=ortak_anahtar)

with pd.ExcelWriter("TEMIZLENMIS_ANALIZ_VERILERI.xlsx") as writer:
    if not vs1.empty:
        vs1.to_excel(writer, sheet_name="1_Uretim_Kalite", index=False)
    if not vs2.empty:
        vs2.to_excel(writer, sheet_name="2_Pol_Uret_Tesp_Kalite", index=False)
    if not vs3.empty:
        vs3.to_excel(writer, sheet_name="3_Havuz_Uret_Kalite", index=False)
    if not vs4.empty:
        vs4.to_excel(writer, sheet_name="4_Havuz_Uretim", index=False)

print("-" * 40)
print("İŞLEM BAŞARILI! EŞLEŞEN SATIR SAYILARI:")
print(f"Set 1 (Üretim+Kalite): {len(vs1)} satır")
print(f"Set 2 (Pol+Üret+Tesp+Kalite): {len(vs2)} satır")
print(f"Set 3 (Havuz+Üret+Kalite): {len(vs3)} satır")
print(f"Set 4 (Havuz+Üretim): {len(vs4)} satır")
