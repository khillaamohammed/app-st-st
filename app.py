import streamlit as st
import pandas as pd
from datetime import datetime
from dbfread import DBF
from difflib import SequenceMatcher

# ---------------------------
# CONFIG
# ---------------------------
st.set_page_config(page_title="ST vs STIT", layout="wide")
st.title(" Analyse ST / STIT")

# ---------------------------
# FONCTIONS UTILES
# ---------------------------

def nettoyer_texte(txt):
    return str(txt).lower().strip()

def nettoyer_adresse(txt):
    return " ".join(str(txt).lower().split())

def similarite(a, b):
    return SequenceMatcher(None, str(a), str(b)).ratio() * 100

import tempfile
from dbfread import DBF
import pandas as pd

def load_dbf(uploaded_file):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".dbf") as tmp:
        tmp.write(uploaded_file.read())
        tmp_path = tmp.name

    dbf = DBF(tmp_path, load=True)
    df = pd.DataFrame(iter(dbf))

    return df
def trouver_code_st_valide(code_st, id_imb_set):
    code_st = str(code_st).strip().upper()

    if code_st in id_imb_set:
        return code_st

    parties = code_st.split('-')
    if len(parties) < 2:
        return None

    for lettre in ["A", "B", "C", "N"]:
        code_mod = f"{parties[0]}-{lettre}-" + "-".join(parties[1:])
        if code_mod in id_imb_set:
            return code_mod

    return None

# ---------------------------
# UPLOAD ST
# ---------------------------
st.header("1. Upload fichier ST (.dbf)")

file_st = st.file_uploader("Choisir fichier ST", type=["dbf"])

# ---------------------------
# CHOIX COMPLÉTUDE
# ---------------------------
choix = st.radio("Choisir complétude", ["1", "2"])

# ---------------------------
# UPLOAD STIT
# ---------------------------
st.header("2. Upload fichiers STIT (.csv)")
files_stit = st.file_uploader("Importer fichiers STIT", type=["csv"], accept_multiple_files=True)

# ---------------------------
# EXECUTION
# ---------------------------
if file_st and files_stit:

    # -------- IMPORT ST --------
    df_st = load_dbf(file_st)

    if 'obs' not in df_st.columns:
        st.error("Colonne 'obs' absente")
        st.stop()

    df_st['obs_clean'] = df_st['obs'].fillna('').apply(nettoyer_texte)

    # -------- FILTRAGE --------
    if df_st['obs_clean'].str.strip().eq('').all():
        df_filtre = df_st.copy()
    else:
        if choix == "1":
            pattern = r'completude\s*1'
        else:
            pattern = r'completude\s*2'

        df_filtre = df_st[df_st['obs_clean'].str.contains(pattern, na=False)]

        if df_filtre.empty:
            df_filtre = df_st.copy()

    # -------- EXCLUSION SRO --------
    if not df_filtre.empty:
        derniere_ligne = df_filtre.iloc[-1]
        if 'sro' in str(derniere_ligne['obs_clean']):
            df_filtre = df_filtre.iloc[:-1]

    # -------- COLONNES --------
    for col in ['code_st', 'code_insee']:
        if col not in df_filtre.columns:
            st.error(f"Colonne manquante: {col}")
            st.stop()

    df_code_st = df_filtre[['code_st']].copy()
    df_code_st['code_st'] = df_code_st['code_st'].astype(str).str.upper().str.strip()

    # -------- IMPORT STIT --------
    df_stit_all = []

    for file in files_stit:
        df_temp = pd.read_csv(file, sep=';', encoding='latin1')
        df_temp.columns = df_temp.columns.str.lower().str.strip()
        df_stit_all.append(df_temp)

    df_stit = pd.concat(df_stit_all, ignore_index=True)

    # -------- NORMALISATION --------
    df_stit['id_imb'] = df_stit['id_imb'].astype(str).str.upper().str.strip()

    id_imb_set = set(df_stit['id_imb'].dropna())

    df_code_st['id_imb_associe'] = df_code_st['code_st'].apply(
        lambda x: trouver_code_st_valide(x, id_imb_set)
    )

    df_code_st['existe'] = df_code_st['id_imb_associe'].notna()

    # -------- ADRESSES ST --------
    for col in ['num_voie', 'ext_num', 'type_voie', 'nom_voie', 'code_post', 'commune']:
        if col not in df_st.columns:
            df_st[col] = ''
        else:
            df_st[col] = df_st[col].fillna('')

    df_st['adresse_st'] = (
        df_st['num_voie'].astype(str) + ' ' +
        df_st['type_voie'].astype(str) + ' ' +
        df_st['nom_voie'].astype(str) + ' ' +
        df_st['code_post'].astype(str) + ' ' +
        df_st['commune'].astype(str)
    ).apply(nettoyer_adresse)

    # -------- ADRESSES STIT --------
    for col in ['numerovoie', 'type_voie', 'nom_voie', 'code_postal', 'ville']:
        if col not in df_stit.columns:
            df_stit[col] = ''
        else:
            df_stit[col] = df_stit[col].fillna('')

    df_stit['adresse_stit'] = (
        df_stit['numerovoie'].astype(str) + ' ' +
        df_stit['type_voie'].astype(str) + ' ' +
        df_stit['nom_voie'].astype(str) + ' ' +
        df_stit['code_postal'].astype(str) + ' ' +
        df_stit['ville'].astype(str)
    ).apply(nettoyer_adresse)

    # -------- FUSION --------
    df_merge = df_code_st.merge(
        df_st[['code_st', 'adresse_st']],
        on='code_st',
        how='left'
    ).merge(
        df_stit[['id_imb', 'adresse_stit']],
        left_on='id_imb_associe',
        right_on='id_imb',
        how='left'
    )

    # -------- COMPARAISON --------
    def comparer(row):
        if row['adresse_st'] == row['adresse_stit']:
            return True
        if similarite(row['adresse_st'], row['adresse_stit']) >= 95:
            return True
        return False

    df_merge['match'] = df_merge.apply(comparer, axis=1)

    # -------- RESULTATS --------
    st.header("📊 Résultats")

    st.write("Total :", len(df_merge))
    st.write("Match OK :", df_merge['match'].sum())
    st.write("Mismatch :", len(df_merge) - df_merge['match'].sum())

    st.dataframe(df_merge.head(50))

    # -------- EXPORT --------
    st.header("📥 Export")

    filename = f"resultat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df_merge.to_excel(filename, index=False)

    with open(filename, "rb") as f:
        st.download_button("Télécharger Excel", f, file_name=filename)
  
