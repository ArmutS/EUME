import pandas as pd
import numpy as np
from scipy import stats

df = pd.read_excel("main.xlsx", sheet_name="1_Uretim_Kalite")
df.columns = df.columns.str.strip()

df["Renk Tonu Farkı"] = pd.to_numeric(df["Renk Tonu Farkı"], errors="coerce").fillna(0)
df["HataVar"] = np.where(df["Renk Tonu Farkı"] > 0, "Var", "Yok")


tablo = pd.crosstab(df["Renk"], df["HataVar"])
chi2, p, dof, expected = stats.chi2_contingency(tablo)

print("Ki-kare p:", p)


for col in ["A/dm2", "Voltaj", "Amper"]:
    var_grubu = pd.to_numeric(
        df.loc[df["HataVar"] == "Var", col], errors="coerce"
    ).dropna()
    yok_grubu = pd.to_numeric(
        df.loc[df["HataVar"] == "Yok", col], errors="coerce"
    ).dropna()

    print("\n", col)
    print("Shapiro Var p:", stats.shapiro(var_grubu).pvalue)
    print("Shapiro Yok p:", stats.shapiro(yok_grubu).pvalue)
    print("Levene p:", stats.levene(var_grubu, yok_grubu, center="median").pvalue)
    print(
        "Mann-Whitney p:",
        stats.mannwhitneyu(var_grubu, yok_grubu, alternative="two-sided").pvalue,
    )
