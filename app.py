import pandas as pd
from google.colab import files
from dbfread import DBF
import unicodedata
import re
 
# ---------------------------
# FONCTIONS UTILES
# ---------------------------
def upload_dbf(message):
    print(message)
    uploaded = files.upload()
    for filename in uploaded.keys():
        if filename.lower().endswith('.dbf'):
            print(f"Fichier DBF sélectionné : {filename}")
            table = DBF(filename, encoding='cp1252')
            df = pd.DataFrame(iter(table))
            df.columns = df.columns.str.strip().str.lower()
            return df, filename
    raise ValueError("Aucun fichier DBF valide fourni.")
 
def nettoyer_texte(texte):
    texte = str(texte).lower()
    texte = unicodedata.normalize('NFKD', texte).encode('ascii','ignore').decode('utf-8')
    return texte.strip()
 
# Fonction de nettoyage des adresses STIT sans supprimer les espaces
def nettoyer_adresse_stit(texte):
    texte = str(texte).lower()
    texte = unicodedata.normalize('NFKD', texte).encode('ascii', 'ignore').decode('utf-8')
    texte = re.sub(r'\(.*?\)', '', texte)  # enlever parenthèses
    texte = texte.replace(',', '').replace('-', ' ')  # remplacer les tirets par des espaces
    texte = re.sub(r'\b(l|la|le|les)\b', '', texte)  # enlever articles (la, le, les, etc.)
    texte = re.sub(r'\s+', ' ', texte)  # garder un seul espace entre mots
    return texte.strip()
 
# ---------------------------
# 1️⃣ IMPORT ET FILTRAGE ST
# ---------------------------
df_st, name_st = upload_dbf("Veuillez sélectionner le fichier ST")
print("Importation terminée.")
 
if 'obs' not in df_st.columns:
    raise ValueError("La colonne 'obs' n'existe pas dans le fichier.")
 
df_st['obs_clean'] = df_st['obs'].fillna('').apply(nettoyer_texte)
 
print("1 - Complétude 1\n2 - Complétude 2")
choix = input("Entrez 1 ou 2 : ").strip()
 
if df_st['obs_clean'].str.strip().eq('').all():
    df_filtre = df_st.copy()
else:
    if choix == "1":
        pattern = r'completude\s*1'
    elif choix == "2":
        pattern = r'completude\s*2'
    else:
        df_filtre = df_st.copy()
        pattern = None
 
    if pattern:
        df_filtre = df_st[df_st['obs_clean'].str.contains(pattern, na=False)]
        if df_filtre.empty:
            df_filtre = df_st.copy()
 
pm = None
 
mask_sro = df_filtre['obs_clean'].str.contains('sro', na=False)
 
if mask_sro.any():
    pm = df_filtre.loc[mask_sro, 'code_st'].iloc[0]
    print(f"PM (SRO) détecté : {pm}")
 
    df_filtre = df_filtre[~mask_sro]
    print("Ligne(s) SRO supprimée(s)")
print(f"Lignes finales : {df_filtre.shape[0]}")
 
# ---------------------------
# EXTRACTION CODE_ST ET CODE_INSEE
# ---------------------------
for col in ['code_st', 'code_insee']:
    if col not in df_filtre.columns:
        raise ValueError(f"Colonne '{col}' manquante dans le fichier filtré.")
 
df_code_st_affichage = df_filtre[['code_st']].copy()
df_code_st_affichage['code_st'] = df_code_st_affichage['code_st'].astype(str).str.strip().str.upper()
df_code_insee_seul = df_filtre[['code_insee']].drop_duplicates()
display(df_code_st_affichage.head(40))
display(df_code_insee_seul.head(40))
 
# ---------------------------
# 2️⃣ IMPORT MULTIPLE EXPORT STIT
# ---------------------------
df_stit_all = []
df_code_insee_unique = df_filtre[['code_insee']].dropna().drop_duplicates()
 
for idx, code_insee in enumerate(df_code_insee_unique['code_insee']):
    print(f"\nSélection du fichier export STIT pour le code INSEE {code_insee} ({idx+1}/{len(df_code_insee_unique)}) :")
    uploaded = files.upload()
    for filename in uploaded.keys():
        if filename.lower().endswith('.csv'):
            df_temp = pd.read_csv(filename, sep=';', encoding='latin1')
            df_temp.columns = df_temp.columns.str.strip().str.lower().str.replace(' ', '_')
            df_temp['code_insee'] = code_insee
            df_stit_all.append(df_temp)
 
df_stit_global = pd.concat(df_stit_all, ignore_index=True)
 
# ---------------------------
# 3️⃣ VÉRIFICATION CODE_ST DANS ID_IMB
# ---------------------------
df_stit_global['id_imb'] = df_stit_global['id_imb'].astype(str).str.strip().str.upper()
id_imb_set = set(df_stit_global['id_imb'].dropna())
lettres_possibles = ["A", "B", "C", "N"]
 
def trouver_code_st_valide(code_st, id_imb_set, lettres_possibles=lettres_possibles):
    code_st = str(code_st).strip().upper()
    if code_st in id_imb_set:
        return code_st
    parties = code_st.split('-')
    if len(parties) < 2:
        return None
    for lettre in lettres_possibles:
        code_mod = f"{parties[0]}-{lettre}-" + "-".join(parties[1:])
        if code_mod in id_imb_set:
            return code_mod
    return None
 
df_code_st_affichage['id_imb_associe'] = df_code_st_affichage['code_st'].apply(
    lambda x: trouver_code_st_valide(x, id_imb_set)
)
df_code_st_affichage['existe_dans_id_imb'] = df_code_st_affichage['id_imb_associe'].notna()
display(df_code_st_affichage.head(40))
 
# ---------------------------
# 4️⃣ FILTRAGE ID VALIDES
# ---------------------------
ids_valides = df_code_st_affichage[df_code_st_affichage['existe_dans_id_imb']]['id_imb_associe'].dropna().tolist()
ids_valides_set = set(ids_valides)
 
# ---------------------------
# 5️⃣ NETTOYAGE ADRESSES ST
# ---------------------------
cols_st = ['code_st','num_voie','ext_num','type_voie','nom_voie','code_post','commune']
df_st_adresse = df_filtre[df_filtre['code_st'].isin(df_code_st_affichage['code_st'])][cols_st].copy()
 
for col in cols_st:
    if col != 'code_st':
        df_st_adresse[col] = df_st_adresse[col].fillna('').apply(nettoyer_adresse_stit)
 
df_st_adresse['num_voie_full'] = (df_st_adresse['num_voie'] + df_st_adresse['ext_num']).str.replace(" ", "")
 
# ---------------------------
# 6️⃣ NETTOYAGE ADRESSES STIT
# ---------------------------
cols_stit = ['id_imb','numerovoie','type_voie','nom_voie','code_postal','ville']
df_stit_adresse = df_stit_global[df_stit_global['id_imb'].isin(ids_valides_set)][cols_stit].copy()
 
# Nettoyage STIT (numerovoie + type_voie + nom_voie + code_postal + ville)
for col in ['numerovoie','type_voie','nom_voie','code_postal','ville']:
    df_stit_adresse[col] = df_stit_adresse[col].fillna('').apply(nettoyer_adresse_stit)
 
# Si numerovoie vide → remplacer par "0"
df_stit_adresse['numerovoie'] = df_stit_adresse['numerovoie'].replace('', '0').str.replace(" ", "")
 
# ---------------------------
# 7️⃣ EXTRACTION TYPE VOIE
# ---------------------------
mapping_types = {
    'boulevard':'boulevard','bd':'boulevard',
    'avenue':'avenue','av':'avenue',
    'rout':'route','rte':'route','route':'route',
    'chemin':'chemin','chem':'chemin',
    'impasse':'impasse','imp':'impasse',
    'place':'place','pl':'place',
    'rue':'rue','r':'rue',
    'allee':'allee','all':'allee',
    'voie':'voie',
    'hameau':'hameau',
    't':'ter',
    'ferme':'ferme',
    'lieu dit':'lieu dit',
    'st':'saint'
}
 
def extraire_type_voie(nom):
    nom = str(nom).strip().lower()
    nom = re.sub(r'\s+', ' ', nom)
    for key in sorted(mapping_types.keys(), key=len, reverse=True):
        if nom.startswith(key + " "):
            return mapping_types[key], nom[len(key):].strip()
    return "", nom
 
df_stit_adresse[['type_voie_mod','nom_voie_mod']] = df_stit_adresse['nom_voie'].apply(
    lambda x: pd.Series(extraire_type_voie(x))
)
 
# fallback
df_stit_adresse['type_voie_mod'] = df_stit_adresse.apply(
    lambda row: row['type_voie_mod'] if row['type_voie_mod'] != "" else row['type_voie'],
    axis=1
)
 
df_st_adresse['nom_voie'] = df_st_adresse['nom_voie'].str.strip()
df_stit_adresse['nom_voie_mod'] = df_stit_adresse['nom_voie_mod'].str.strip()
df_stit_adresse['type_voie_mod'] = df_stit_adresse['type_voie_mod'].str.strip()
 
# ---------------------------
# 8️⃣ APERÇU FINAL
# ---------------------------
print("ST :")
display(df_st_adresse.head(40))
 
print("STIT :")
display(df_stit_adresse[['id_imb','numerovoie','type_voie_mod','nom_voie_mod','code_postal','ville']].head(40))
# -----------------------------
# 1️⃣ Fusion ST / STIT
# -----------------------------
# Nettoyage pour fusion
df_stit_adresse['id_imb_clean'] = df_stit_adresse['id_imb'].str.replace(r'-[A-Z]-', '-', regex=True).str.upper().str.strip()
df_st_adresse['code_st_clean'] = df_st_adresse['code_st'].str.upper().str.strip()
 
# Fusion ST / STIT
df_comparaison = pd.merge(
    df_st_adresse,
    df_stit_adresse,
    left_on='code_st_clean',
    right_on='id_imb_clean',
    how='inner'
)
 
print(f"Lignes après fusion : {df_comparaison.shape[0]}")
 
# -----------------------------
# 2️⃣ Comparaison multi-niveaux avec les colonnes renommées
# -----------------------------
df_comparaison['num_st_norm'] = df_comparaison['num_voie_full'].apply(normaliser_numero)
df_comparaison['num_stit_norm'] = df_comparaison['numerovoie'].apply(normaliser_numero)
 
def comparer_adresses(row):
    # Ville et code postal
    if row['commune'] == row['ville'] and row['code_post'] == row['code_postal']:
        # Nom de voie
        if row['nom_voie_x'] == row['nom_voie_mod']:
            # Type de voie
            if row['type_voie_x'] != row['type_voie_mod']:
                return 'DIFF_TYPE_VOIE'
            # Numéro de voie
            elif row['num_st_norm'] != row['num_stit_norm']:
                return 'DIFF_NUM_VOIE'
            else:
                return 'OK'
        else:
            return 'DIFF_NOM_VOIE'
    else:
        return 'DIFF_VILLE_CP'
 
df_comparaison['comparaison_finale'] = df_comparaison.apply(comparer_adresses, axis=1)
 
# -----------------------------
# 3️⃣ Affichage
# -----------------------------
display(df_comparaison[[
    'code_st', 'commune', 'ville', 'code_post', 'code_postal',
    'nom_voie_x', 'nom_voie_mod', 'type_voie_x', 'type_voie_mod',
    'num_voie_full', 'numerovoie', 'comparaison_finale'
]])
 
nb_diff = (df_comparaison['comparaison_finale'] != 'OK').sum()
print(f"Nombre de lignes avec différences : {nb_diff}")
