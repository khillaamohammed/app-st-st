import streamlit as st
import pandas as pd
from dbfread import DBF
import unicodedata
import re
from io import BytesIO

# ---------------------------
# FONCTIONS
# ---------------------------
def nettoyer_texte(texte):
    texte = str(texte).lower()
    texte = unicodedata.normalize('NFKD', texte).encode('ascii','ignore').decode('utf-8')
    return texte.strip()

def nettoyer_adresse_stit(texte):
    texte = str(texte).lower()
    texte = unicodedata.normalize('NFKD', texte).encode('ascii', 'ignore').decode('utf-8')
    texte = re.sub(r'\(.*?\)', '', texte)
    texte = texte.replace(',', '').replace('-', ' ')
    texte = re.sub(r'\b(l|la|le|les)\b', '', texte)
    texte = re.sub(r'\s+', ' ', texte)
    return texte.strip()

def extraire_type_voie(nom):
    mapping_types = {
        'boulevard':'boulevard','bd':'boulevard',
        'avenue':'avenue','av':'avenue',
        'route':'route','rte':'route',
        'chemin':'chemin',
        'impasse':'impasse',
        'place':'place',
        'rue':'rue',
        'allee':'allee',
        'voie':'voie'
    }
    nom = str(nom).strip().lower()
    for key in sorted(mapping_types.keys(), key=len, reverse=True):
        if nom.startswith(key):
            return mapping_types[key], nom[len(key):].strip()
    return "", nom

def normaliser_numero(x):
    return str(x).replace(" ", "").lower()

# ---------------------------
# INTERFACE STREAMLIT
# ---------------------------
st.title("Analyse ST / STIT (Streamlit Cloud)")

st_file = st.file_uploader("Upload ST (.dbf)", type=["dbf"])
choix = st.selectbox("Complétude", ["1", "2"])
stit_files = st.file_uploader("Upload STIT (.csv)", type=["csv"], accept_multiple_files=True)

# ---------------------------
# TRAITEMENT
# ---------------------------
if st.button("Lancer l'analyse"):

    if not st_file or not stit_files:
        st.error("Veuillez charger tous les fichiers")
        st.stop()

    # ---------------------------
    # ST (DBF)
    # ---------------------------
    table = DBF(st_file, encoding='cp1252')
    df_st = pd.DataFrame(iter(table))
    df_st.columns = df_st.columns.str.strip().str.lower()

    df_st['obs_clean'] = df_st['obs'].fillna('').apply(nettoyer_texte)

    if df_st['obs_clean'].str.strip().eq('').all():
        df_filtre = df_st.copy()
    else:
        pattern = r'completude\s*1' if choix == "1" else r'completude\s*2'
        df_filtre = df_st[df_st['obs_clean'].str.contains(pattern, na=False)]
        if df_filtre.empty:
            df_filtre = df_st.copy()

    mask_sro = df_filtre['obs_clean'].str.contains('sro', na=False)

    if mask_sro.any():
        df_filtre = df_filtre[~mask_sro]

    df_code_st_affichage = df_filtre[['code_st']].copy()
    df_code_st_affichage['code_st'] = df_code_st_affichage['code_st'].astype(str).str.upper()

    # ---------------------------
    # STIT (CSV)
    # ---------------------------
    df_stit_all = []

    for file in stit_files:
        df_temp = pd.read_csv(file, sep=';', encoding='latin1')
        df_temp.columns = df_temp.columns.str.strip().str.lower().str.replace(' ', '_')
        df_stit_all.append(df_temp)

    df_stit_global = pd.concat(df_stit_all, ignore_index=True)

    df_stit_global['id_imb'] = df_stit_global['id_imb'].astype(str).str.upper()
    id_imb_set = set(df_stit_global['id_imb'])

    # ---------------------------
    # matching
    # ---------------------------
    def trouver_code_st_valide(code_st):
        code_st = str(code_st).upper()
        if code_st in id_imb_set:
            return code_st
        return None

    df_code_st_affichage['id_imb_associe'] = df_code_st_affichage['code_st'].apply(trouver_code_st_valide)
    df_code_st_affichage['existe'] = df_code_st_affichage['id_imb_associe'].notna()

    ids_valides = df_code_st_affichage[df_code_st_affichage['existe']]['id_imb_associe'].tolist()

    # ---------------------------
    # ADRESSES ST
    # ---------------------------
    cols_st = ['code_st','num_voie','ext_num','type_voie','nom_voie','code_post','commune']
    df_st_adresse = df_filtre[cols_st].copy()

    for col in cols_st:
        if col != 'code_st':
            df_st_adresse[col] = df_st_adresse[col].fillna('').apply(nettoyer_adresse_stit)

    df_st_adresse['num_voie_full'] = (df_st_adresse['num_voie'] + df_st_adresse['ext_num']).str.replace(" ", "")

    # ---------------------------
    # ADRESSES STIT
    # ---------------------------
    cols_stit = ['id_imb','numerovoie','type_voie','nom_voie','code_postal','ville']
    df_stit_adresse = df_stit_global[df_stit_global['id_imb'].isin(ids_valides)][cols_stit].copy()

    for col in cols_stit:
        df_stit_adresse[col] = df_stit_adresse[col].fillna('').apply(nettoyer_adresse_stit)

    df_stit_adresse['numerovoie'] = df_stit_adresse['numerovoie'].replace('', '0')

    df_stit_adresse[['type_voie_mod','nom_voie_mod']] = df_stit_adresse['nom_voie'].apply(
        lambda x: pd.Series(extraire_type_voie(x))
    )

    df_stit_adresse['id_imb_clean'] = df_stit_adresse['id_imb'].str.replace(r'-[A-Z]-','-',regex=True)

    # ---------------------------
    # MERGE
    # ---------------------------
    df_comparaison = pd.merge(
        df_st_adresse,
        df_stit_adresse,
        left_on='code_st',
        right_on='id_imb_clean',
        how='inner'
    )

    df_comparaison['num_st_norm'] = df_comparaison['num_voie_full'].apply(normaliser_numero)
    df_comparaison['num_stit_norm'] = df_comparaison['numerovoie'].apply(normaliser_numero)

    def comparer(row):
        if row['commune'] == row['ville'] and row['code_post'] == row['code_postal']:
            if row['nom_voie_x'] == row['nom_voie_mod']:
                if row['type_voie_x'] != row['type_voie_mod']:
                    return 'DIFF_TYPE_VOIE'
                elif row['num_st_norm'] != row['num_stit_norm']:
                    return 'DIFF_NUM_VOIE'
                else:
                    return 'OK'
            else:
                return 'DIFF_NOM_VOIE'
        else:
            return 'DIFF_VILLE_CP'

    df_comparaison['comparaison_finale'] = df_comparaison.apply(comparer, axis=1)

    # ---------------------------
    # RESULTAT
    # ---------------------------
    st.success("Analyse terminée ✅")
    st.dataframe(df_comparaison)

    # ---------------------------
    # EXPORT EXCEL
    # ---------------------------
    output = BytesIO()
    df_comparaison.to_excel(output, index=False)
    output.seek(0)

    st.download_button(
        "📥 Télécharger Excel",
        data=output,
        file_name="resultat.xlsx"
    )
