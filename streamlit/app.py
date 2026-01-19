import streamlit as st
import pandas as pd
import os
import s3fs
import json
from io import StringIO

# =================================================
# CONFIGURATION PAGE
# =================================================
st.set_page_config(
    page_title="Weather Data Dashboard",
    page_icon="🌤️",
    layout="wide"
)

# =================================================
# CONFIGURATION MINIO
# =================================================
bucket_name = os.environ.get("MINIO_BUCKET", "weather-spark-streaming")
access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
endpoint_url = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

st.title("🌤️ Dashboard Weather Data ")
st.markdown("---")

# =================================================
# CONNEXION À MINIO
# =================================================
@st.cache_resource
def get_s3_filesystem():
    """Crée une connexion S3FS vers MinIO"""
    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={'endpoint_url': endpoint_url},
        use_ssl=False
    )
    return fs

# =================================================
# CHARGEMENT DES DONNÉES JSON
# =================================================
@st.cache_data(ttl=60)
def load_data_from_minio():
    """Charge les données JSON depuis MinIO"""
    try:
        fs = get_s3_filesystem()
        folder_path = f"{bucket_name}/weather_enriched"
        
        # Lister tous les fichiers JSON (exclure _spark_metadata)
        all_files = fs.ls(folder_path)
        json_files = [f for f in all_files 
                      if f.endswith('.json') 
                      and '_spark_metadata' not in f
                      and fs.size(f) > 0]
        
        if not json_files:
            st.warning(f"❌ Aucun fichier JSON trouvé dans {folder_path}")
            return None
        
        st.info(f"📁 {len(json_files)} fichier(s) JSON détecté(s)")
        
        # Lire tous les fichiers JSON
        all_data = []
        errors = 0
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for idx, file_path in enumerate(json_files):
            try:
                # Mise à jour de la progression
                progress = (idx + 1) / len(json_files)
                progress_bar.progress(progress)
                status_text.text(f"Chargement... {idx + 1}/{len(json_files)}")
                
                # Lire le fichier JSON
                with fs.open(file_path, 'r') as f:
                    content = f.read()
                    
                    # Chaque ligne est un objet JSON (JSON Lines format)
                    for line in content.strip().split('\n'):
                        if line.strip():
                            try:
                                data = json.loads(line)
                                all_data.append(data)
                            except json.JSONDecodeError:
                                # Si ce n'est pas JSON Lines, essayer de parser tout le fichier
                                try:
                                    data = json.loads(content)
                                    if isinstance(data, list):
                                        all_data.extend(data)
                                    else:
                                        all_data.append(data)
                                    break
                                except:
                                    pass
                
            except Exception as e:
                errors += 1
                if errors < 5:  # Afficher max 5 erreurs
                    st.warning(f"⚠️ Erreur lecture {file_path.split('/')[-1]}: {str(e)[:100]}")
        
        progress_bar.empty()
        status_text.empty()
        
        if not all_data:
            st.error("❌ Aucune donnée valide trouvée")
            return None
        
        # Créer le DataFrame
        df = pd.DataFrame(all_data)
        
        # ===== CORRECTION DU TIMESTAMP =====
        if 'timestamp' in df.columns:
            try:
                # Convertir en numérique si c'est une string
                if df['timestamp'].dtype == 'object':
                    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
                
                # Déterminer l'unité (secondes ou millisecondes)
                sample_timestamp = df['timestamp'].dropna().iloc[0] if not df['timestamp'].dropna().empty else None
                
                if sample_timestamp is not None:
                    # Si le timestamp est > 1e12, c'est en millisecondes
                    if sample_timestamp > 1e12:
                        df['timestamp_dt'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
                    # Si le timestamp est > 1e10, c'est en secondes
                    elif sample_timestamp > 1e10:
                        df['timestamp_dt'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
                    else:
                        # Essayer format ISO ou autre
                        df['timestamp_dt'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    
                    # Supprimer les dates invalides
                    df = df[df['timestamp_dt'].notna()]
                    
            except Exception as e:
                st.warning(f"⚠️ Impossible de convertir timestamp: {e}")
                # Créer une colonne timestamp_dt vide pour éviter les erreurs
                df['timestamp_dt'] = pd.NaT
        
        # Nettoyer les colonnes vides
        df = df.dropna(axis=1, how='all')
        
        return df
        
    except Exception as e:
        st.error(f"❌ Erreur générale: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None

# =================================================
# CHARGEMENT
# =================================================
with st.spinner("⏳ Chargement des données depuis MinIO..."):
    df = load_data_from_minio()

if df is None or df.empty:
    st.error("❌ Aucune donnée disponible")
    
    # Debug info
    with st.expander("🔍 Informations de debug", expanded=True):
        st.code(f"""
Bucket: {bucket_name}
Chemin: weather_enriched
Endpoint: {endpoint_url}
Access Key: {access_key}
        """)
        
        try:
            fs = get_s3_filesystem()
            buckets = fs.ls('')
            st.write("📦 Buckets:", buckets)
        except Exception as e:
            st.error(f"Erreur connexion: {e}")
    
    st.stop()

st.success(f"✅ {len(df)} enregistrements chargés depuis {len(df.columns)} colonnes !")

# =================================================
# MÉTRIQUES PRINCIPALES
# =================================================
st.subheader(" Aperçu des données")

col1, col2, col3= st.columns(3)

with col1:
    st.metric("📝 Enregistrements", f"{len(df):,}")

with col2:
    if 'city_name' in df.columns:
        unique_cities = df['city_name'].nunique()
        st.metric("🏙️ Villes", unique_cities)
    elif 'city' in df.columns:
        unique_cities = df['city'].nunique()
        st.metric("🏙️ Villes", unique_cities)
    else:
        st.metric("🏙️ Villes", "N/A")

with col3:
    st.metric("📋 Colonnes", len(df.columns))



# =================================================
# COLONNES DISPONIBLES
# =================================================
with st.expander("📋 Colonnes disponibles", expanded=False):
    st.write(list(df.columns))
    st.write("\n**Types de données:**")
    st.write(df.dtypes)

# =================================================
# APERÇU DES DONNÉES
# =================================================
st.subheader("🔍 Aperçu des données brutes")
st.dataframe(df.head(100), use_container_width=True)

# =================================================
# NORMALISATION DES NOMS DE COLONNES
# =================================================
# Uniformiser les noms de colonnes
if 'temp' in df.columns and 'temperature' not in df.columns:
    df['temperature'] = df['temp']
if 'city' in df.columns and 'city_name' not in df.columns:
    df['city_name'] = df['city']

# =================================================
# GRAPHIQUES
# =================================================
import plotly.express as px
import plotly.graph_objects as go

st.markdown("---")
st.subheader("📈 Visualisations")

# Vérifier les colonnes nécessaires
has_temp = 'temperature' in df.columns
has_city = 'city_name' in df.columns
has_time = 'timestamp_dt' in df.columns

# GRAPHIQUE 1: Température dans le temps
if has_temp and has_time:
    st.markdown("### 🌡️ Évolution de la température")
    
    df_sorted = df.sort_values('timestamp_dt')
    
    if has_city:
        # Permettre la sélection des villes
        all_cities = df['city_name'].unique()
        selected_cities = st.multiselect(
            "Sélectionner les villes à afficher",
            options=all_cities,
            default=all_cities[:5] if len(all_cities) > 5 else all_cities
        )
        
        if selected_cities:
            df_filtered = df_sorted[df_sorted['city_name'].isin(selected_cities)]
            
            fig1 = px.line(
                df_filtered,
                x='timestamp_dt',
                y='temperature',
                color='city_name',
                title="Température par ville au fil du temps",
                labels={
                    'temperature': 'Température (°C)',
                    'timestamp_dt': 'Date/Heure',
                    'city_name': 'Ville'
                }
            )
        else:
            st.warning("Veuillez sélectionner au moins une ville")
            fig1 = None
    else:
        fig1 = px.line(
            df_sorted,
            x='timestamp_dt',
            y='temperature',
            title="Évolution de la température",
            labels={
                'temperature': 'Température (°C)',
                'timestamp_dt': 'Date/Heure'
            }
        )
    
    if fig1:
        fig1.update_layout(height=500, hovermode='x unified')
        st.plotly_chart(fig1, use_container_width=True)

# GRAPHIQUE 2: Distribution et Box Plot
if has_temp:
    st.markdown("---")
    st.markdown("### 📊 Distribution des températures")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig2 = px.histogram(
            df,
            x='temperature',
            nbins=30,
            title="Distribution des températures",
            labels={'temperature': 'Température (°C)', 'count': 'Fréquence'},
            color_discrete_sequence=['#FF6B6B']
        )
        fig2.update_layout(showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)
    
    with col2:
        if has_city:
            fig3 = px.box(
                df,
                x='city_name',
                y='temperature',
                title="Températures par ville (Box Plot)",
                labels={'temperature': 'Température (°C)', 'city_name': 'Ville'},
                color='city_name'
            )
            fig3.update_layout(showlegend=False)
            fig3.update_xaxes(tickangle=-45)
            st.plotly_chart(fig3, use_container_width=True)
# GRAPHIQUE 3: Statistiques par ville (TABLEAU UNIQUEMENT)
if has_temp and has_city:
    st.markdown("---")
    st.markdown("### 📊 Statistiques par ville")
    
    city_stats = df.groupby('city_name')['temperature'].agg([
        ('Moyenne', 'mean'),
        ('Min', 'min'),
        ('Max', 'max'),
        ('Écart-type', 'std')
    ]).round(2)
    
    # Affichage du tableau uniquement
    st.dataframe(city_stats, use_container_width=True)


# =================================================
# ALERTES MÉTÉO
# =================================================
st.markdown("---")
st.subheader("⚠️ Alertes météo")

if 'alert_type' in df.columns:
    alerts = df[df['alert_type'] != "NORMAL"]
    
    if not alerts.empty:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.error(f"🚨 **{len(alerts)}** alerte(s) détectée(s)")
            
            # Comptage par type d'alerte
            alert_counts = alerts['alert_type'].value_counts()
            st.write("**Par type:**")
            for alert_type, count in alert_counts.items():
                st.write(f"- {alert_type}: {count}")
        
        with col2:
            # Graphique des alertes
            fig_alert = px.pie(
                values=alert_counts.values,
                names=alert_counts.index,
                title="Répartition des types d'alertes",
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            st.plotly_chart(fig_alert, use_container_width=True)
        
        # Tableau des alertes
        st.markdown("#### 📋 Détail des alertes")
        st.dataframe(
            alerts.sort_values('timestamp_dt', ascending=False) if 'timestamp_dt' in alerts.columns else alerts,
            use_container_width=True
        )
    else:
        st.success("✅ Aucune alerte météo en cours")
else:
    st.info("ℹ️ Pas de colonne 'alert_type' dans les données")

# =================================================
# STATISTIQUES GLOBALES
# =================================================
if has_temp:
    st.markdown("---")
    st.subheader("📊 Statistiques globales")
    
    stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
    
    with stat_col1:
        st.metric("🌡️ Temp. Moyenne", f"{df['temperature'].mean():.1f}°C")
    
    with stat_col2:
        st.metric("🔥 Temp. Max", f"{df['temperature'].max():.1f}°C")
    
    with stat_col3:
        st.metric("❄️ Temp. Min", f"{df['temperature'].min():.1f}°C")
    
    with stat_col4:
        st.metric("📏 Écart-type", f"{df['temperature'].std():.1f}°C")

# =================================================
# FILTRES AVANCÉS
# =================================================
st.markdown("---")
st.subheader("🔍 Filtrage avancé")

filter_col1, filter_col2 = st.columns(2)

with filter_col1:
    if has_temp:
        temp_range = st.slider(
            "Filtrer par température (°C)",
            float(df['temperature'].min()),
            float(df['temperature'].max()),
            (float(df['temperature'].min()), float(df['temperature'].max()))
        )
        df_filtered = df[(df['temperature'] >= temp_range[0]) & (df['temperature'] <= temp_range[1])]
    else:
        df_filtered = df

with filter_col2:
    if has_city:
        selected_cities_filter = st.multiselect(
            "Filtrer par ville",
            options=df['city_name'].unique(),
            default=None
        )
        if selected_cities_filter:
            df_filtered = df_filtered[df_filtered['city_name'].isin(selected_cities_filter)]

st.write(f"**{len(df_filtered)} enregistrement(s)** après filtrage")
st.dataframe(df_filtered, use_container_width=True)

# =================================================
# EXPORT
# =================================================
st.markdown("---")
st.subheader("💾 Export des données")

col1, col2 = st.columns(2)

with col1:
    # Export CSV
    csv = df_filtered.to_csv(index=False)
    st.download_button(
        label="📥 Télécharger en CSV",
        data=csv,
        file_name=f"weather_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

with col2:
    # Export JSON
    json_str = df_filtered.to_json(orient='records', indent=2)
    st.download_button(
        label="📥 Télécharger en JSON",
        data=json_str,
        file_name=f"weather_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json",
        mime="application/json"
    )

# =================================================
# FOOTER
# =================================================
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    🌤️ Weather Data Dashboard
</div>
""", unsafe_allow_html=True)