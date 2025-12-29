import os
import time
import glob
import pandas as pd
import gspread
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from google.oauth2.service_account import Credentials

try:
    from fake_useragent import UserAgent
except ImportError:
    class UserAgent:
        def __init__(self): self.random = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

# ================= CONFIGURACIÓN =================
# Asegurar que el script corre en su propio directorio
os.chdir(os.path.dirname(os.path.abspath(__file__)))

NOMBRE_ARCHIVO_JSON = 'credenciales_google.json' 

# ID DE LA HOJA DE CÁLCULO (GOOGLE SHEET NATIVA)
ID_GOOGLE_SHEET = '1lTzIjgBBThfwERHH-_yRPWXDFcvGspwTzANngG-WjRY'
# =================================================

# --- CONEXIÓN GOOGLE SHEETS (ROBUSTA) ---
def conectar_sheets():
    print("🔌 Conectando a Google Sheets...")
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(NOMBRE_ARCHIVO_JSON, scopes=scopes)
        gc = gspread.authorize(creds)
        return gc
    except Exception as e:
        print(f"❌ ERROR CRÍTICO DE CONEXIÓN: {e}")
        return None

# --- NAVEGADOR PARA DESCARGA (CONFIGURADO PARA NUBE/HEADLESS) ---
def configurar_navegador_descarga():
    chrome_options = Options()
    
    # OPCIONES VITALES PARA GITHUB ACTIONS (LINUX/HEADLESS)
    chrome_options.add_argument("--headless=new") # Sin interfaz gráfica
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Configuración de descargas en modo headless
    prefs = {
        "download.default_directory": os.getcwd(),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

# --- NAVEGADOR PARA SCRAPING (MODO SIGILO + HEADLESS) ---
def configurar_navegador_sigilo():
    chrome_options = Options()
    
    # MODO HEADLESS COMPATIBLE CON GITHUB
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Evasión de detección de bots
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    ua = UserAgent()
    chrome_options.add_argument(f'user-agent={ua.random}')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Script extra para ocultar WebDriver
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

# --- PASO 1: DESCARGAR MERCADO PÚBLICO ---
def descargar_excel_diario():
    print("⬇️ [1/5] Descargando listado de Mercado Público (Modo Nube)...")
    driver = configurar_navegador_descarga()
    wait = WebDriverWait(driver, 30) # Tiempo extra por si la nube es lenta
    archivo_descargado = None
    carpeta_actual = os.getcwd()

    try:
        driver.get("https://www.mercadopublico.cl/portal/Modules/Site/Busquedas/BuscadorAvanzado.aspx?qs=1")
        
        # Interacción con filtros
        wait.until(EC.element_to_be_clickable((By.ID, "chkRegion"))).click()
        time.sleep(1)
        Select(wait.until(EC.visibility_of_element_located((By.ID, "ddlRegion")))).select_by_value("7") # Maule
        
        driver.find_element(By.ID, "chkEstado").click()
        time.sleep(1)
        Select(wait.until(EC.visibility_of_element_located((By.ID, "ddlAdquisitionState")))).select_by_value("5") # Publicada
        
        driver.find_element(By.ID, "btnBusqueda").click()
        time.sleep(8) # Pausa generosa para carga de resultados
        
        wait.until(EC.element_to_be_clickable((By.ID, "_LbtBottomDownloadExcel"))).click()
        print("⏳ Esperando descarga...")

        # Esperar archivo (Hasta 60 segs)
        timeout = 0
        while timeout < 60:
            archivos = glob.glob(os.path.join(carpeta_actual, "*.xls*"))
            validos = [f for f in archivos if "crdownload" not in f and not f.endswith('.xlsx')]
            if validos:
                archivo_descargado = max(validos, key=os.path.getctime)
                print(f"✅ Descargado: {os.path.basename(archivo_descargado)}")
                break
            time.sleep(1)
            timeout += 1
            
    except Exception as e:
        print(f"❌ Error descarga: {e}")
    finally:
        driver.quit()
    return archivo_descargado

# --- PROCESO PRINCIPAL ---
def ejecutar_proceso():
    ruta_xls = descargar_excel_diario()
    if not ruta_xls: return

    print("📊 [2/5] Cruzando datos con Google Sheets...")
    
    # Leer Excel (suprimiendo warnings de xlrd)
    try: 
        import warnings
        warnings.filterwarnings('ignore', category=UserWarning, module='xlrd')
        df_diario = pd.read_excel(ruta_xls, engine='xlrd')
    except: 
        df_diario = pd.read_excel(ruta_xls)
    
    # Normalizar columnas
    df_diario.columns = df_diario.columns.str.strip()
    col_num = next((c for c in df_diario.columns if "Número" in c or "Numero" in c), None)
    col_nom = next((c for c in df_diario.columns if "Nombre" in c and "Organismo" not in c), None)
    col_comp = next((c for c in df_diario.columns if "Nombre" in c and "Organismo" in c), "Comprador")
    col_cierre = next((c for c in df_diario.columns if "Fecha" in c and "Cierre" in c), None)

    if not all([col_num, col_nom, col_cierre]):
         print("❌ Error: Faltan columnas clave en el Excel descargado.")
         return

    # Conectar a Google Sheet
    gc = conectar_sheets()
    if not gc: return

    try:
        sheet = gc.open_by_key(ID_GOOGLE_SHEET).sheet1
        
        data_master = sheet.get_all_records()
        df_master = pd.DataFrame(data_master)
        
        ids_existentes = []
        if not df_master.empty and 'Número' in df_master.columns:
             ids_existentes = df_master['Número'].astype(str).unique().tolist()
        
    except Exception as e:
        print(f"❌ Error abriendo el Sheet: {e}")
        return

    # Filtrar nuevos
    df_diario[col_num] = df_diario[col_num].astype(str)
    df_nuevos = df_diario[~df_diario[col_num].isin(ids_existentes)].copy()
    
    if df_nuevos.empty:
        print("😴 No hay nada nuevo para agregar hoy.")
    else:
        print(f"✨ [3/5] Encontradas {len(df_nuevos)} licitaciones nuevas. Extrayendo info extra...")
        
        driver = configurar_navegador_sigilo()
        wait = WebDriverWait(driver, 15)
        base_url = "http://www.mercadopublico.cl/Procurement/Modules/RFB/DetailsAcquisition.aspx?idlicitacion="
        
        filas_para_agregar = []
        
        for index, row in df_nuevos.iterrows():
            lic_id = str(row[col_num])
            link = base_url + lic_id
            print(f"   🕵️ {lic_id}...", end=" ")
            
            desc = "No encontrada"
            fecha_pub = "No encontrada"
            
            try:
                driver.get(link)
                # Manejo de bloqueo 403
                if "Forbidden" in driver.title or "403" in driver.title:
                     print("⛔ BLOQUEO. Pausa 60s...", end=" ")
                     time.sleep(60)
                     driver.get(link)

                # Extraer Descripción
                try:
                    elem_desc = wait.until(EC.presence_of_element_located((By.ID, "lblFicha1Descripcion")))
                    desc = elem_desc.text.strip().replace("\n", " ")[:2500]
                except: pass
                
                # Extraer Fecha Publicación
                try:
                    elem_pub = driver.find_element(By.ID, "lblFicha3Publicacion")
                    fecha_pub = elem_pub.text.strip()
                except: pass
                
                print("✅")
                
            except Exception as e:
                print(f"❌")
            
            # Formatear fecha cierre
            fecha_cierre_raw = row[col_cierre]
            fecha_cierre_str = str(fecha_cierre_raw)
            if isinstance(fecha_cierre_raw, pd.Timestamp):
                 fecha_cierre_str = fecha_cierre_raw.strftime("%d-%m-%Y %H:%M:%S")

            # ORDEN: A=Link, B=Num, C=Comp, D=Nom, E=Desc, F=Cierre, G=Pub
            nueva_fila = [
                link,
                lic_id,
                str(row.get(col_comp, "N/A")),
                str(row[col_nom]),
                desc,
                fecha_cierre_str,
                fecha_pub
            ]
            filas_para_agregar.append(nueva_fila)
            time.sleep(random.uniform(2.0, 4.0)) # Pausa corta
            
        driver.quit()
        
        print(f"🚀 [4/5] Subiendo {len(filas_para_agregar)} filas a la nube...")
        try:
            sheet.append_rows(filas_para_agregar, value_input_option='USER_ENTERED')
            print("✅ Base maestra actualizada.")
        except Exception as e:
            print(f"❌ Error al subir filas: {e}")

    # --- PASO 3: LIMPIEZA DE VENCIDOS ---
    print("🧹 [5/5] Limpiando licitaciones vencidas...")
    
    # Manejo de archivo temporal
    def borrar_temporal():
        try:
            if ruta_xls and os.path.exists(ruta_xls):
                os.remove(ruta_xls)
        except: pass

    try:
        valores_hoja = sheet.get_all_values()
    except Exception as e:
        print(f"❌ Error al leer para limpieza: {e}")
        borrar_temporal()
        return

    if len(valores_hoja) <= 1:
        borrar_temporal()
        return

    encabezados = valores_hoja.pop(0)
    hoy = datetime.now().date()
    filas_a_conservar = [encabezados]
    eliminados = 0

    for fila in valores_hoja:
        fecha_txt = fila[5] if len(fila) > 5 else ""
        conservar = True
        if len(fecha_txt) > 8:
            try:
                fecha_txt_limpia = fecha_txt.replace("/", "-").split(" ")[0].strip()
                try:
                    fecha_obj = datetime.strptime(fecha_txt_limpia, "%d-%m-%Y").date()
                except ValueError:
                    fecha_obj = datetime.strptime(fecha_txt_limpia, "%Y-%m-%d").date()

                if fecha_obj < hoy:
                    conservar = False
            except: pass 
        
        if conservar:
            filas_a_conservar.append(fila)
        else:
            eliminados += 1
            
    if eliminados > 0:
        print(f"   👋 Eliminando {eliminados} licitaciones antiguas...")
        try:
            sheet.clear()
            sheet.update(filas_a_conservar, value_input_option='USER_ENTERED')
            print("✨ Limpieza lista.")
        except Exception as e:
            print(f"❌ Error actualizando hoja limpia: {e}")
    else:
        print("✅ No hay nada vencido para borrar.")

    borrar_temporal()
    print("\n🎉 --- FIN DEL PROCESO --- 🎉")

if __name__ == "__main__":
    ejecutar_proceso()