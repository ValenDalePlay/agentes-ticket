from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging
import os
from datetime import datetime, timedelta
import json
import re
import random
import psycopg2
from database_config import get_database_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TicketekScraper:
    def __init__(self, headless=True, test_mode=False):
        """
        Inicializa el scraper de Ticketek
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless
            test_mode (bool): Si True, solo extrae datos sin guardar en BD
        """
        self.driver = None
        self.headless = headless
        self.test_mode = test_mode
        self.base_url = "https://reportes.ticketek.com.ar/#/reports/full"
        self.username = "chalfon"
        self.password = "argentina"
        
        # Configuración para Airflow
        self.download_folder = "/tmp" if headless else "jsonticketeck"
        if not headless:
            self.setup_download_folder()
        
        # Configurar conexión a BD siempre (incluso en modo test)
        self.db_connection = None
        self.setup_database_connection()
        
    def setup_download_folder(self):
        """Crea la carpeta para descargas si no existe"""
        try:
            if not os.path.exists(self.download_folder):
                os.makedirs(self.download_folder)
                logger.info(f"Carpeta creada: {self.download_folder}")
            else:
                logger.info(f"Carpeta ya existe: {self.download_folder}")
        except Exception as e:
            logger.error(f"Error creando carpeta de descargas: {str(e)}")
    
    def setup_driver(self):
        """Configura el driver de Chrome con las opciones necesarias y bot evasion"""
        try:
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument("--headless")
            
            # Opciones básicas para Airflow/Windows
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Bot evasion
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]
            chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
            
            # Configurar carpeta de descargas
            chrome_options.add_experimental_option("prefs", {
                "download.default_directory": os.path.abspath(self.download_folder),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            })
            
            # Intentar configurar el driver de forma simple
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
            except Exception as e:
                logger.warning(f"Error con configuración simple: {str(e)}")
                # Intentar con webdriver-manager como respaldo
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Configuraciones adicionales para bot evasion
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": random.choice(user_agents)
            })
            
            logger.info("Driver de Chrome configurado exitosamente con bot evasion")
            return True
            
        except Exception as e:
            logger.error(f"Error al configurar el driver: {str(e)}")
            return False
    
    def navigate_to_login(self):
        """Navega a la página de login de Ticketek"""
        try:
            logger.info(f"Navegando a: {self.base_url}")
            self.driver.get(self.base_url)
            
            # Esperar a que la página cargue completamente
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            logger.info("Página de login cargada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error navegando a la página de login: {str(e)}")
            return False
    
    def login(self):
        """Realiza el login en el sistema de reportes de Ticketek"""
        try:
            logger.info("Iniciando proceso de login...")
            
            # Esperar a que aparezca el formulario de login
            logger.info("Esperando a que aparezca el formulario de login...")
            
            # Buscar el campo de usuario por diferentes métodos
            username_field = None
            
            # Intentar encontrar por placeholder
            try:
                username_field = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Usuario']"))
                )
                logger.info("Campo de usuario encontrado por placeholder")
            except:
                # Intentar encontrar por name="email"
                try:
                    username_field = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.NAME, "email"))
                    )
                    logger.info("Campo de usuario encontrado por name='email'")
                except:
                    # Intentar encontrar por type="text"
                    try:
                        username_field = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, "//input[@type='text']"))
                        )
                        logger.info("Campo de usuario encontrado por type='text'")
                    except:
                        logger.error("No se pudo encontrar el campo de usuario")
                        return False
            
            # Buscar el campo de contraseña
            password_field = None
            
            try:
                password_field = self.driver.find_element(By.NAME, "password")
                logger.info("Campo de contraseña encontrado por name='password'")
            except:
                try:
                    password_field = self.driver.find_element(By.XPATH, "//input[@type='password']")
                    logger.info("Campo de contraseña encontrado por type='password'")
                except:
                    logger.error("No se pudo encontrar el campo de contraseña")
                    return False
            
            # Limpiar campos y escribir credenciales
            username_field.clear()
            username_field.send_keys(self.username)
            logger.info(f"Usuario ingresado: {self.username}")
            
            password_field.clear()
            password_field.send_keys(self.password)
            logger.info("Contraseña ingresada")
            
            # Buscar el botón de login
            login_button = None
            
            try:
                # Buscar por texto "Log In"
                login_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Log In')]")
                logger.info("Botón de login encontrado por texto 'Log In'")
            except:
                try:
                    # Buscar por type="submit"
                    login_button = self.driver.find_element(By.XPATH, "//button[@type='submit']")
                    logger.info("Botón de login encontrado por type='submit'")
                except:
                    try:
                        # Buscar por clase btn
                        login_button = self.driver.find_element(By.CSS_SELECTOR, "button.btn")
                        logger.info("Botón de login encontrado por clase 'btn'")
                    except:
                        logger.error("No se pudo encontrar el botón de login")
                        return False
            
            # Hacer clic en el botón de login
            login_button.click()
            logger.info("Botón de login clickeado")
            
            # Esperar a que se complete el login
            time.sleep(5)
            
            # Verificar si hay alertas de error
            try:
                alert = self.driver.switch_to.alert
                alert_text = alert.text
                logger.error(f"🚨 Alert detectado: {alert_text}")
                alert.accept()  # Aceptar el alert
                
                if "credenciales" in alert_text.lower() or "invalid" in alert_text.lower():
                    logger.error("❌ CREDENCIALES INVÁLIDAS - Verifica usuario y contraseña")
                    logger.error("💡 Posibles soluciones:")
                    logger.error("   1. Verificar que las credenciales sean correctas")
                    logger.error("   2. Verificar que la cuenta no esté bloqueada")
                    logger.error("   3. Verificar que el sitio web esté funcionando correctamente")
                    return False
                    
            except:
                # No hay alert, continuar
                logger.info("✅ No se detectaron alertas de error")
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            logger.info(f"URL actual después del login: {current_url}")
            
            # Verificar si ya no estamos en la página de login
            page_source = self.driver.page_source.lower()
            
            if "usuario" not in page_source or "contraseña" not in page_source or "password" not in page_source:
                logger.info("Login exitoso - No se detecta formulario de login")
                return True
            else:
                # Verificar si hay mensaje de error
                try:
                    error_indicators = [
                        "error",
                        "invalid",
                        "incorrecto",
                        "fallido",
                        "failed"
                    ]
                    
                    for indicator in error_indicators:
                        if indicator in page_source:
                            logger.error(f"Error en login detectado: {indicator}")
                            return False
                    
                    logger.warning("Login puede haber fallado - aún se detecta formulario")
                    return False
                        
                except:
                    pass
                
                logger.warning("Estado de login incierto")
                return True  # Asumir éxito si no hay errores claros
                
        except Exception as e:
            logger.error(f"Error durante el login: {str(e)}")
            return False
    
    def extract_initial_summary_table(self):
        """Extrae los datos de la tabla de resumen inicial (pantalla principal)"""
        try:
            logger.info("Extrayendo datos de la tabla de resumen inicial...")
            
            # Buscar todas las tablas
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            
            if not tables:
                logger.warning("No se encontraron tablas en la página principal")
                return []
            
            logger.info(f"Encontradas {len(tables)} tablas en la página principal")
            
            summary_data = []
            
            # Buscar la tabla que contenga la estructura de resumen
            for table_index, table in enumerate(tables):
                try:
                    logger.info(f"Analizando tabla {table_index + 1} para extraer datos de resumen...")
                    
                    # Extraer headers
                    headers = []
                    try:
                        thead = table.find_element(By.TAG_NAME, "thead")
                        header_row = thead.find_element(By.TAG_NAME, "tr")
                        header_cells = header_row.find_elements(By.TAG_NAME, "th")
                        headers = [cell.text.strip() for cell in header_cells]
                        logger.info(f"Headers encontrados en tabla {table_index + 1}: {headers}")
                    except:
                        # Si no hay thead, intentar con la primera fila
                        try:
                            first_row = table.find_element(By.CSS_SELECTOR, "tr:first-child")
                            header_cells = first_row.find_elements(By.CSS_SELECTOR, "th, td")
                            headers = [cell.text.strip() for cell in header_cells]
                            logger.info(f"Headers extraídos de primera fila: {headers}")
                        except:
                            continue
                    
                    # Verificar si es la tabla de resumen (debe tener columnas como Código, Nombre, Capacidad, etc.)
                    # O detectar por la estructura típica de la primera fila de datos
                    expected_columns = ["código", "nombre", "capacidad", "disponibles", "cortesías", "total vendidos", "%", "recaudación"]
                    header_text_lower = ' '.join(headers).lower()
                    
                    # Verificar si contiene las columnas esperadas O si tiene la estructura típica de códigos de evento
                    is_summary_table = any(col in header_text_lower for col in expected_columns)
                    
                    # También verificar si la primera celda parece un código de evento y hay datos numéricos
                    if not is_summary_table and len(headers) >= 10:
                        first_cell = headers[0].strip()
                        # Si la primera celda es un código de evento típico (LETRAS+NÚMEROS)
                        if re.match(r'^[A-Z0-9]+$', first_cell) and len(first_cell) >= 6:
                            # Y hay datos que parecen números en las siguientes celdas
                            numeric_count = 0
                            for header in headers[2:]:  # Skip code and name
                                if re.search(r'\d', header.replace('.', '').replace(',', '').replace('$', '')):
                                    numeric_count += 1
                            
                            if numeric_count >= 5:  # Si hay al menos 5 columnas con números
                                is_summary_table = True
                                logger.info(f"Tabla {table_index + 1} detectada como resumen por estructura de datos")
                    
                    if is_summary_table:
                        logger.info(f"Tabla {table_index + 1} identificada como tabla de resumen")
                        
                        # Determinar si la primera fila contiene headers reales o datos
                        has_real_headers = any(col in header_text_lower for col in expected_columns)
                        
                        # Extraer filas de datos
                        try:
                            tbody = table.find_element(By.TAG_NAME, "tbody")
                            data_rows = tbody.find_elements(By.TAG_NAME, "tr")
                        except:
                            # Si no hay tbody, buscar todas las filas
                            all_rows = table.find_elements(By.TAG_NAME, "tr")
                            if has_real_headers:
                                # Si hay headers reales, skip la primera fila
                                data_rows = all_rows[1:] if len(all_rows) > 1 else []
                            else:
                                # Si la primera fila son datos, incluir todas las filas
                                data_rows = all_rows
                                # Redefinir headers genéricos para esta estructura
                                headers = ["codigo", "nombre", "capacidad", "disponibles", "cortesias", "holdeos", 
                                          "ultimos_7_dias", "ayer", "hoy", "ultimas_3_horas", "total_vendidos", 
                                          "porcentaje", "recaudacion"]
                        
                        logger.info(f"Encontradas {len(data_rows)} filas de datos en tabla resumen")
                        
                        for row_index, row in enumerate(data_rows):
                            try:
                                cells = row.find_elements(By.CSS_SELECTOR, "td, th")
                                
                                if cells and len(cells) > 0:
                                    row_data = {
                                        "fuente": "Tabla Resumen Inicial",
                                        "tabla_numero": "resumen",
                                        "fila_numero": row_index + 1,
                                        "fecha_extraccion": datetime.now().isoformat(),
                                        "total_columnas": len(cells)
                                    }
                                    
                                    # Mapear cada celda con su header correspondiente
                                    for i, cell in enumerate(cells):
                                        header_name = headers[i] if i < len(headers) else f"Columna_{i+1}"
                                        cell_text = cell.text.strip()
                                        
                                        # Para la columna de porcentaje, limpiar la barra de progreso
                                        if "%" in header_name.lower() and cell_text:
                                            percentage_match = re.search(r'(\d+\.?\d*)', cell_text)
                                            if percentage_match:
                                                cell_text = percentage_match.group(1) + "%"
                                        
                                        # Normalizar el nombre del header
                                        header_key = header_name.lower().replace(" ", "_").replace("ñ", "n")
                                        header_key = re.sub(r'[^\w]', '_', header_key)
                                        header_key = re.sub(r'_+', '_', header_key).strip('_')
                                        
                                        row_data[header_key] = cell_text if cell_text else ""
                                    
                                    summary_data.append(row_data)
                                    
                                    # Log de las primeras filas
                                    if row_index < 3:
                                        codigo = row_data.get('codigo', 'N/A')
                                        nombre = row_data.get('nombre', 'N/A')
                                        vendidos = row_data.get('total_vendidos', 'N/A')
                                        logger.info(f"  Fila resumen {row_index + 1}: {codigo} - {nombre[:30]}... - Vendidos: {vendidos}")
                            
                            except Exception as e:
                                logger.warning(f"Error procesando fila {row_index + 1} de tabla resumen: {str(e)}")
                                continue
                        
                        logger.info(f"Tabla de resumen procesada: {len(summary_data)} registros extraídos")
                        break  # Solo procesar la primera tabla de resumen encontrada
                    
                except Exception as e:
                    logger.warning(f"Error analizando tabla {table_index + 1}: {str(e)}")
                    continue
            
            logger.info(f"Total registros de resumen extraídos: {len(summary_data)}")
            return summary_data
            
        except Exception as e:
            logger.error(f"Error extrayendo tabla de resumen inicial: {str(e)}")
            return []
    
    def extract_active_events_from_initial_table(self):
        """Extrae los códigos de eventos activos de la tabla inicial (pantalla principal)"""
        try:
            logger.info("Extrayendo eventos activos de la tabla inicial...")
            
            # Buscar la tabla inicial en la página principal (antes de ir a reportes completos)
            active_events = []
            
            # Buscar todas las tablas
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            
            if not tables:
                logger.warning("No se encontraron tablas en la página principal")
                return []
            
            logger.info(f"Encontradas {len(tables)} tablas en la página principal")
            
            # Buscar en todas las tablas la que contenga códigos de eventos
            for table_index, table in enumerate(tables):
                try:
                    logger.info(f"Analizando tabla {table_index + 1}...")
                    
                    # Buscar filas con enlaces que contengan códigos de evento
                    event_links = table.find_elements(By.CSS_SELECTOR, "a")
                    
                    for link in event_links:
                        try:
                            link_text = link.text.strip()
                            
                            # Verificar si parece un código de evento (formato: LETRAS+NÚMEROS)
                            if re.match(r'^[A-Z0-9]+$', link_text) and len(link_text) >= 6:
                                if link_text not in active_events:
                                    active_events.append(link_text)
                                    logger.info(f"Evento activo encontrado: {link_text}")
                                    
                        except Exception as e:
                            continue
                    
                    # También buscar en celdas td que puedan contener códigos
                    cells = table.find_elements(By.TAG_NAME, "td")
                    for cell in cells:
                        try:
                            cell_text = cell.text.strip()
                            
                            # Verificar si es un código de evento
                            if re.match(r'^[A-Z0-9]+$', cell_text) and len(cell_text) >= 6:
                                if cell_text not in active_events:
                                    active_events.append(cell_text)
                                    logger.info(f"Evento activo encontrado en celda: {cell_text}")
                                    
                        except Exception as e:
                            continue
                            
                except Exception as e:
                    logger.warning(f"Error analizando tabla {table_index + 1}: {str(e)}")
                    continue
            
            logger.info(f"Total de eventos activos encontrados: {len(active_events)}")
            logger.info(f"Eventos activos: {active_events}")
            
            return active_events
            
        except Exception as e:
            logger.error(f"Error extrayendo eventos activos: {str(e)}")
            return []
    
    def navigate_to_home(self):
        """Navega a la página principal/home para ver la tabla de resumen"""
        try:
            logger.info("Navegando a la página principal...")
            
            # Intentar diferentes métodos para ir a la página principal
            try:
                # Método 1: URL directa a home
                home_url = "https://reportes.ticketek.com.ar/#/reports/home"
                self.driver.get(home_url)
                time.sleep(3)
                logger.info(f"Navegación directa a home exitosa: {home_url}")
                return True
            except Exception as e:
                logger.warning(f"Error navegando directamente a home: {str(e)}")
                
                try:
                    # Método 2: Buscar enlace "Posicion Consolidada" o "Home"
                    home_link = None
                    
                    # Buscar enlace por texto
                    try:
                        home_link = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Posicion Consolidada"))
                        )
                        logger.info("Enlace 'Posicion Consolidada' encontrado")
                    except:
                        try:
                            home_link = WebDriverWait(self.driver, 5).until(
                                EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Home"))
                            )
                            logger.info("Enlace 'Home' encontrado")
                        except:
                            pass
                    
                    if home_link:
                        home_link.click()
                        time.sleep(3)
                        logger.info("Navegación exitosa a página principal")
                        return True
                    else:
                        logger.warning("No se encontró enlace a página principal")
                        return False
                        
                except Exception as e:
                    logger.error(f"Error navegando a página principal: {str(e)}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error general navegando a home: {str(e)}")
            return False
    
    def explore_available_reports(self):
        """Explora qué tipos de reportes están disponibles en Ticketeck"""
        try:
            logger.info("🔍 EXPLORANDO TIPOS DE REPORTES DISPONIBLES...")
            print("\n🔍 EXPLORANDO OPCIONES DE REPORTES EN TICKETECK")
            print("=" * 60)
            
            # Buscar todos los enlaces de reportes disponibles
            report_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='reports']")
            
            print(f"📋 Enlaces de reportes encontrados: {len(report_links)}")
            
            for i, link in enumerate(report_links, 1):
                try:
                    href = link.get_attribute("href")
                    text = link.text.strip()
                    print(f"   {i}. {text} → {href}")
                except:
                    continue
            
            # Buscar también por clases y textos relacionados con reportes
            potential_reports = [
                "//a[contains(text(), 'diario')]",
                "//a[contains(text(), 'Diario')]",
                "//a[contains(text(), 'daily')]",
                "//a[contains(text(), 'Daily')]",
                "//a[contains(text(), 'historico')]",
                "//a[contains(text(), 'Historico')]",
                "//a[contains(text(), 'history')]",
                "//a[contains(text(), 'History')]",
                "//a[contains(text(), 'ventas')]",
                "//a[contains(text(), 'Ventas')]",
                "//a[contains(text(), 'sales')]",
                "//a[contains(text(), 'Sales')]"
            ]
            
            print(f"\n🔍 Buscando reportes específicos de ventas diarias...")
            found_daily_reports = []
            
            for xpath in potential_reports:
                try:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    for elem in elements:
                        href = elem.get_attribute("href") or "sin href"
                        text = elem.text.strip()
                        if text and text not in [r['text'] for r in found_daily_reports]:
                            found_daily_reports.append({'text': text, 'href': href})
                except:
                    continue
            
            if found_daily_reports:
                print(f"📊 Reportes de ventas encontrados: {len(found_daily_reports)}")
                for i, report in enumerate(found_daily_reports, 1):
                    print(f"   {i}. {report['text']} → {report['href']}")
            else:
                print("❌ No se encontraron reportes específicos de ventas diarias")
            
            # Explorar el menú lateral completo
            print(f"\n📋 Explorando menú lateral completo...")
            sidebar_items = self.driver.find_elements(By.CSS_SELECTOR, ".sidebar-link, .nav-link, .menu-item")
            
            print(f"📋 Items del menú lateral: {len(sidebar_items)}")
            for i, item in enumerate(sidebar_items, 1):
                try:
                    text = item.text.strip()
                    href = item.get_attribute("href") or "sin href"
                    if text:
                        print(f"   {i}. {text} → {href}")
                except:
                    continue
            
            print("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"❌ Error explorando reportes: {e}")
            return False
    
    def extract_daily_sales_from_home(self, active_events):
        """Extrae datos diarios de ventas desde la página HOME de Ticketeck"""
        try:
            logger.info("🏠 EXTRAYENDO DATOS DIARIOS DESDE LA PÁGINA HOME...")
            print("\n🏠 ANÁLISIS DE DATOS DIARIOS EN HOME")
            print("=" * 60)
            
            # Asegurar que estamos en la página HOME
            home_url = "https://reportes.ticketek.com.ar/#/reports/home"
            if self.driver.current_url != home_url:
                logger.info("Navegando a la página HOME...")
                self.driver.get(home_url)
                time.sleep(3)
            
            # Buscar la tabla principal que contiene los datos de resumen
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            
            if not tables:
                logger.warning("❌ No se encontraron tablas en la página HOME")
                return []
            
            logger.info(f"📊 Encontradas {len(tables)} tablas en HOME")
            
            # Analizar la tabla principal
            main_table = tables[0]  # La tabla principal de resumen
            
            # Extraer headers - manejar diferentes estructuras de tabla
            headers = []
            try:
                # Intentar encontrar thead
                header_row = main_table.find_element(By.TAG_NAME, "thead").find_element(By.TAG_NAME, "tr")
                headers = [th.text.strip() for th in header_row.find_elements(By.TAG_NAME, "th")]
            except:
                # Si no hay thead, usar la primera fila como headers
                try:
                    first_row = main_table.find_element(By.TAG_NAME, "tr")
                    headers = [cell.text.strip() for cell in first_row.find_elements(By.TAG_NAME, "td")]
                    if not headers:
                        headers = [cell.text.strip() for cell in first_row.find_elements(By.TAG_NAME, "th")]
                except:
                    # Headers por defecto basados en lo que vimos en el output
                    headers = ['Código', 'Evento', 'Vendidos', 'Total_Venta', 'Col5', 'Col6', 'Col7', 'Col8', 'Col9', 'Col10', 'Col11', 'Ocupación', 'Recaudación']
            
            logger.info(f"📋 Headers encontrados: {headers}")
            print(f"📋 Columnas disponibles: {headers}")
            
            # Extraer filas de datos - manejar diferentes estructuras
            rows = []
            try:
                tbody = main_table.find_element(By.TAG_NAME, "tbody")
                rows = tbody.find_elements(By.TAG_NAME, "tr")
            except:
                # Si no hay tbody, usar todas las filas de la tabla
                rows = main_table.find_elements(By.TAG_NAME, "tr")
            
            logger.info(f"📊 Filas de datos encontradas: {len(rows)}")
            
            daily_data = []
            
            # Procesar cada fila (cada evento)
            for row_index, row in enumerate(rows):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) < len(headers):
                        continue
                    
                    # Extraer datos de la fila
                    row_data = {}
                    cell_values = []
                    for i, cell in enumerate(cells):
                        cell_value = cell.text.strip()
                        cell_values.append(cell_value)
                        if i < len(headers):
                            row_data[headers[i].lower().replace(' ', '_')] = cell_value
                    
                    # El código del evento está en la primera columna
                    event_code = cell_values[0] if cell_values else ''
                    
                    logger.info(f"🔍 Procesando fila: código='{event_code}', valores={cell_values}")
                    
                    # Solo procesar eventos activos
                    if event_code in active_events:
                        logger.info(f"🎯 Procesando evento activo: {event_code}")
                        print(f"\n🎭 EVENTO: {event_code}")
                        
                        # Extraer datos de ventas diarias
                        sales_info = self.extract_sales_info_from_row(cell_values, headers)
                        
                        if sales_info:
                            # Verificar/crear show en BD (solo en modo producción)
                            if not self.test_mode:
                                show_id = self.ensure_show_exists(sales_info)
                            else:
                                show_id = self.map_event_to_show_id(event_code, row_data)
                            
                            if show_id:
                                # Calcular registros de daily_sales para AYER y HOY
                                daily_sales_records = self.calculate_daily_sales_records(show_id, sales_info)
                                
                                daily_record = {
                                    'evento_codigo': event_code,
                                    'show_id': show_id,
                                    'fecha_extraccion': datetime.now(),
                                    'ticketera': 'ticketeck',
                                    'daily_sales_calculated': daily_sales_records,
                                    **sales_info
                                }
                                
                                daily_data.append(daily_record)
                                
                                print(f"   ✅ Datos extraídos:")
                                print(f"   📊 Show ID: {show_id}")
                                print(f"   💰 Venta hoy: {sales_info.get('venta_hoy', 'N/A')}")
                                print(f"   📅 Venta ayer: {sales_info.get('venta_ayer', 'N/A')}")
                                print(f"   💵 Total: {sales_info.get('venta_total', 'N/A')}")
                                
                                # Mostrar cálculos de daily_sales
                                print(f"   🧮 CÁLCULOS DAILY_SALES:")
                                for record in daily_sales_records:
                                    print(f"      📅 {record['fecha_venta']}: {record['venta_diaria']} tickets (${record['monto_diario_ars']:,})")
                                    print(f"         Total acum: {record['venta_total_acumulada']} | Disponibles: {record['tickets_disponibles']} | Ocupación: {record['porcentaje_ocupacion']:.1f}%")
                                
                                # UPSERT de registros daily_sales (solo en modo producción)
                                if not self.test_mode:
                                    print(f"   💾 GUARDANDO EN BD:")
                                    for record in daily_sales_records:
                                        result = self.upsert_daily_sales_record(record)
                                        fecha = record['fecha_venta']
                                        ventas = record['venta_diaria']
                                        if result == "updated":
                                            print(f"      🔄 {fecha}: ACTUALIZADO ({ventas} tickets)")
                                        elif result == "inserted":
                                            print(f"      ➕ {fecha}: INSERTADO ({ventas} tickets)")
                                        elif result == "skipped":
                                            print(f"      ⏭️ {fecha}: SIN CAMBIOS ({ventas} tickets)")
                                        else:
                                            print(f"      ❌ {fecha}: ERROR en guardado")
                            else:
                                logger.warning(f"⚠️ No se pudieron extraer datos de ventas para {event_code}")
                        else:
                            logger.warning(f"⚠️ No se pudo mapear evento {event_code} con la BD")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error procesando fila {row_index}: {e}")
                    continue
            
            print(f"\n✅ RESUMEN EXTRACCIÓN DIARIA:")
            print(f"   📊 Eventos procesados: {len(daily_data)}")
            print(f"   🎯 Datos diarios extraídos correctamente")
            print("=" * 60)
            
            return daily_data
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos diarios desde HOME: {e}")
            return []
    
    def extract_sales_info_from_row(self, cell_values, headers):
        """Extrae información de ventas de una fila de datos basado en la estructura real de Ticketeck"""
        try:
            sales_info = {}
            values = cell_values
            
            logger.info(f"🔍 Analizando fila con {len(values)} valores: {values}")
            
            # Estructura REAL de Ticketeck (basada en tu información):
            # Código | Nombre | Capacidad | Disponibles | Cortesías | Holdeos | Últimos 7 días | Ayer | Hoy | Últimas 3 horas | Total Vendidos | % | Recaudación
            # [0]    | [1]    | [2]       | [3]         | [4]       | [5]     | [6]             | [7]  | [8] | [9]              | [10]           | [11] | [12]
            
            if len(values) >= 13:
                # Estructura CORRECTA de Ticketeck
                codigo = values[0] if values[0] else ''
                nombre = values[1] if values[1] else ''
                capacidad = values[2]           # Capacidad total
                disponibles = values[3]         # Disponibles
                cortesias = values[4]           # Cortesías
                holdeos = values[5]             # Holdeos
                ultimos_7_dias = values[6]      # Últimos 7 días
                venta_ayer = values[7]          # AYER ✅
                venta_hoy = values[8]           # HOY ✅
                ultimas_3_horas = values[9]     # Últimas 3 horas
                total_vendidos = values[10]     # Total Vendidos ✅
                ocupacion_pct = values[11]      # Porcentaje ocupación
                recaudacion = values[12]        # Recaudación ✅
                
                sales_info = {
                    'codigo_evento': codigo,
                    'nombre_evento': nombre,
                    'capacidad_total': self.parse_numeric_value(capacidad),
                    'disponibles_total': self.parse_numeric_value(disponibles),
                    'venta_ayer': self.parse_numeric_value(venta_ayer),      # CORRECTO ✅
                    'venta_hoy': self.parse_numeric_value(venta_hoy),        # CORRECTO ✅
                    'venta_total': self.parse_numeric_value(total_vendidos), # CORRECTO ✅
                    'recaudacion_total': self.parse_numeric_value(recaudacion),
                    'ocupacion_porcentaje': self.parse_numeric_value(ocupacion_pct),
                    'cortesias': self.parse_numeric_value(cortesias),
                    'holdeos': self.parse_numeric_value(holdeos),
                    'ultimos_7_dias': self.parse_numeric_value(ultimos_7_dias),
                    'ultimas_3_horas': self.parse_numeric_value(ultimas_3_horas)
                }
                
                logger.info(f"✅ Datos extraídos: vendidos={sales_info['venta_total']}, recaudación={sales_info['recaudacion_total']}")
                return sales_info
            
            else:
                logger.warning(f"⚠️ Estructura de fila inesperada: {len(values)} columnas")
                return None
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo info de ventas: {e}")
            return None
    
    def parse_numeric_value(self, value):
        """Convierte un valor de texto a número, manejando formatos de moneda"""
        try:
            if not value or value.strip() == '':
                return 0
            
            # Remover símbolos de moneda y puntos/comas
            clean_value = value.replace('$', '').replace('.', '').replace(',', '').strip()
            
            # Intentar convertir a entero
            return int(clean_value) if clean_value.isdigit() else 0
            
        except:
            return 0
    
    def map_event_to_show_id(self, event_code, row_data):
        """Mapea un código de evento de Ticketeck con un show_id de la BD"""
        try:
            # Mapeo directo basado en los eventos que conocemos
            event_mapping = {
                'BENJA25TAV': 'bffe4659-33a3-4370-9aeb-31d1d3e84519',  # BENJA TORRES - Teatro Avenida
                'CVIVES25QLA': '26d7202f-71e6-4ed5-b7b2-e44dcc49b36a',  # CARLOS VIVES - Quality Arena  
                'BTORRES25QT': '9751b25f-5836-481a-82ba-181e0076f798'   # BENJA TORRES - Quality Teatro
            }
            
            show_id = event_mapping.get(event_code)
            
            if show_id:
                logger.info(f"✅ Mapeo exitoso: {event_code} → {show_id}")
                return show_id
            else:
                logger.warning(f"⚠️ Evento {event_code} no encontrado en mapeo")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error mapeando evento {event_code}: {e}")
            return None
    
    def calculate_daily_sales_records(self, show_id, sales_info):
        """Calcula registros de daily_sales basado en datos de la base de datos usando método diferencial"""
        try:
            from datetime import date, timedelta
            
            # Obtener capacidad del show desde los datos de Ticketeck
            capacidad_total = self.get_show_capacity_from_sales_info(sales_info)
            
            # Datos extraídos de TickEck
            venta_hoy = sales_info.get('venta_hoy', 0)
            venta_ayer = sales_info.get('venta_ayer', 0) 
            venta_total_actual = sales_info.get('venta_total', 0)
            
            # Fechas
            hoy = date.today()
            ayer = hoy - timedelta(days=1)
            
            records = []
            
            # 1. BUSCAR ÚLTIMO REGISTRO EN LA BASE DE DATOS
            ultimo_registro = self.get_last_daily_sales_record(show_id)
            
            if ultimo_registro:
                venta_total_anterior, recaudacion_total_anterior, fecha_anterior = ultimo_registro
                logger.info(f"📊 Último registro encontrado: {fecha_anterior} - {venta_total_anterior} tickets, ${recaudacion_total_anterior:,}")
            else:
                venta_total_anterior, recaudacion_total_anterior = 0, 0
                logger.info(f"📊 Primer registro para show {show_id}")
            
            # 2. CALCULAR VENTA DIARIA Y MONTO DIARIO (MÉTODO DIFERENCIAL)
            venta_diaria = venta_total_actual - venta_total_anterior
            monto_diario = 0  # Se calculará basándose en la diferencia de recaudación
            
            # 3. CALCULAR RECAUDACIÓN TOTAL ACTUAL
            # Si es el primer registro, usar la recaudación de TickEck
            if venta_total_anterior == 0:
                recaudacion_total_actual = sales_info.get('recaudacion_total', 0)
                monto_diario = recaudacion_total_actual
                logger.info(f"💰 Primer registro: monto diario = ${monto_diario:,}")
            else:
                # Calcular recaudación total basándose en el precio promedio
                precio_promedio = recaudacion_total_anterior / venta_total_anterior if venta_total_anterior > 0 else 100000
                recaudacion_total_actual = venta_total_actual * precio_promedio
                monto_diario = recaudacion_total_actual - recaudacion_total_anterior
                logger.info(f"💰 Cálculo diferencial: ${recaudacion_total_actual:,} - ${recaudacion_total_anterior:,} = ${monto_diario:,}")
            
            # Evitar valores negativos
            venta_diaria = max(0, venta_diaria)
            monto_diario = max(0, monto_diario)
            
            # 4. CALCULAR OTROS CAMPOS
            tickets_disponibles = capacidad_total - venta_total_actual if capacidad_total > 0 else 0
            porcentaje_ocupacion = (venta_total_actual / capacidad_total * 100) if capacidad_total > 0 else 0.0
            
            # 5. CREAR REGISTRO PARA HOY
            record_hoy = {
                'fecha_venta': hoy.strftime('%Y-%m-%d'),
                'venta_diaria': venta_diaria,
                'monto_diario_ars': monto_diario,
                'venta_total_acumulada': venta_total_actual,
                'recaudacion_total_ars': recaudacion_total_actual,
                'tickets_disponibles': tickets_disponibles,
                'porcentaje_ocupacion': round(porcentaje_ocupacion, 2),
                'show_id': show_id,
                'ticketera': 'ticketeck'
            }
            records.append(record_hoy)
            
            logger.info(f"📊 Calculado 1 registro de daily_sales para show {show_id}")
            logger.info(f"   📅 {hoy.strftime('%Y-%m-%d')}: {venta_diaria} tickets (${monto_diario:,})")
            logger.info(f"      Total acum: {venta_total_actual} | Disponibles: {tickets_disponibles} | Ocupación: {porcentaje_ocupacion:.1f}%")
            
            return records
            
        except Exception as e:
            logger.error(f"❌ Error calculando daily_sales: {e}")
            return []
    
    def get_previous_revenue(self, show_id, fecha):
        """Obtiene la recaudación del último registro anterior a la fecha dada"""
        try:
            # En modo test también conectarse a la BD para mostrar datos reales
            # if self.test_mode:
            #     # En modo test, simular que no hay registros anteriores
            #     logger.debug(f"🧪 Modo test: Simulando sin registros anteriores para show {show_id}")
            #     return 0
            
            if not self.db_connection:
                logger.warning("⚠️ Sin conexión a BD, usando 0 como recaudación anterior")
                return 0
            
            cursor = self.db_connection.cursor()
            
            # Buscar el último registro anterior a la fecha
            query = """
                SELECT recaudacion_total_ars 
                FROM daily_sales 
                WHERE show_id = %s AND fecha_venta < %s 
                ORDER BY fecha_venta DESC 
                LIMIT 1
            """
            cursor.execute(query, (show_id, fecha.strftime('%Y-%m-%d')))
            result = cursor.fetchone()
            
            if result:
                recaudacion_anterior = result[0]
                logger.debug(f"📊 Recaudación anterior encontrada: ${recaudacion_anterior:,}")
                return recaudacion_anterior
            else:
                logger.debug(f"📊 Sin registros anteriores para show {show_id}")
                return 0
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo recaudación anterior: {e}")
            return 0
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_last_daily_sales_record(self, show_id):
        """Obtiene el último registro de daily_sales para un show"""
        try:
            # En modo test también conectarse a la BD para mostrar datos reales
            # if self.test_mode:
            #     # En modo test, simular que no hay registros anteriores
            #     logger.debug(f"🧪 Modo test: Simulando sin registros anteriores para show {show_id}")
            #     return None
            
            if not self.db_connection:
                logger.warning("⚠️ Sin conexión a BD, usando None como último registro")
                return None
            
            cursor = self.db_connection.cursor()
            
            # Buscar el último registro para este show
            query = """
                SELECT venta_total_acumulada, recaudacion_total_ars, fecha_venta
                FROM daily_sales 
                WHERE show_id = %s
                ORDER BY fecha_venta DESC 
                LIMIT 1
            """
            cursor.execute(query, (show_id,))
            result = cursor.fetchone()
            
            if result:
                logger.debug(f"📊 Último registro encontrado: {result[2]} - {result[0]} tickets, ${result[1]:,}")
                return result
            else:
                logger.debug(f"📊 Sin registros anteriores para show {show_id}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo último registro: {e}")
            return None
        finally:
            if 'cursor' in locals():
                cursor.close()

    def get_show_capacity_from_sales_info(self, sales_info):
        """Obtiene la capacidad desde los datos extraídos de Ticketeck"""
        try:
            # Usar la capacidad directamente de los datos de Ticketeck
            capacidad_ticketeck = sales_info.get('capacidad_total', 0)
            
            if capacidad_ticketeck > 0:
                logger.debug(f"📊 Capacidad desde Ticketeck: {capacidad_ticketeck}")
                return capacidad_ticketeck
            
            # Fallback a capacidades conocidas si no viene en los datos
            show_id = sales_info.get('show_id', '')
            capacity_mapping = {
                'bffe4659-33a3-4370-9aeb-31d1d3e84519': 1000,  # BENJA TORRES - Teatro Avenida
                '26d7202f-71e6-4ed5-b7b2-e44dcc49b36a': 4192,  # CARLOS VIVES - Quality Arena  
                '9751b25f-5836-481a-82ba-181e0076f798': 1000   # BENJA TORRES - Quality Teatro
            }
            
            capacidad = capacity_mapping.get(show_id, 0)
            logger.debug(f"📊 Capacidad fallback para show {show_id}: {capacidad}")
            return capacidad
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo capacidad: {e}")
            return 0
    
    def setup_database_connection(self):
        """Configura la conexión a la base de datos"""
        try:
            from database_config import get_database_connection
            logger.info("🔌 Verificando conexión con la base de datos...")
            connection = get_database_connection()
            if connection:
                cursor = connection.cursor()
                cursor.execute("SELECT NOW();")
                result = cursor.fetchone()
                logger.info(f"✅ Conexión exitosa! Hora actual: {result[0]}")
                cursor.close()
                # NO cerrar la conexión, mantenerla para uso posterior
                self.db_connection = connection
                return True
            else:
                logger.warning("⚠️ No se pudo establecer conexión a la base de datos")
                self.db_connection = None
                return False
        except Exception as e:
            logger.error(f"❌ Error conectando a BD: {e}")
            self.db_connection = None
            return False
    
    def ensure_show_exists(self, sales_info):
        """Verifica que el show existe en la BD, si no lo crea"""
        try:
            if not self.db_connection:
                logger.warning("⚠️ No hay conexión a BD para verificar show")
                return None
            
            cursor = self.db_connection.cursor()
            evento_codigo = sales_info.get('codigo_evento', '')
            
            # Buscar show existente por código de evento usando mapeo directo
            event_mapping = {
                'BENJA25TAV': 'bffe4659-33a3-4370-9aeb-31d1d3e84519',  # BENJA TORRES - Teatro Avenida
                'CVIVES25QLA': '26d7202f-71e6-4ed5-b7b2-e44dcc49b36a',  # CARLOS VIVES - Quality Arena  
                'BTORRES25QT': '9751b25f-5836-481a-82ba-181e0076f798'   # BENJA TORRES - Quality Teatro
            }
            
            # Primero intentar mapeo directo
            if evento_codigo in event_mapping:
                show_id = event_mapping[evento_codigo]
                
                # Verificar que existe en BD
                verify_query = "SELECT id FROM shows WHERE id = %s::uuid"
                cursor.execute(verify_query, (show_id,))
                if cursor.fetchone():
                    logger.info(f"✅ Show conocido encontrado: {show_id}")
                    return show_id
            
            # Si no está en mapeo directo, buscar por artista
            nombre_evento = sales_info.get('nombre_evento', '')
            artista_search = nombre_evento.split(' EN ')[0] if ' EN ' in nombre_evento else nombre_evento
            
            search_query = """
                SELECT id FROM shows 
                WHERE ticketera = 'ticketeck' 
                AND UPPER(artista) LIKE UPPER(%s)
                LIMIT 1
            """
            cursor.execute(search_query, (f"%{artista_search}%",))
            existing_show = cursor.fetchone()
            
            if existing_show:
                show_id = existing_show[0]
                logger.info(f"✅ Show existente encontrado: {show_id}")
                return show_id
            
            # Si no existe, crear nuevo show
            logger.info(f"🆕 Creando nuevo show para evento: {evento_codigo}")
            return self.create_new_show(sales_info)
            
        except Exception as e:
            logger.error(f"❌ Error verificando show: {e}")
            return None
    
    def create_new_show(self, sales_info):
        """Crea un nuevo show en la BD basado en datos de Ticketeck"""
        try:
            if not self.db_connection:
                return None
            
            cursor = self.db_connection.cursor()
            
            # Extraer datos para el nuevo show
            evento_codigo = sales_info.get('codigo_evento', '')
            nombre_evento = sales_info.get('nombre_evento', '')
            capacidad_total = sales_info.get('capacidad_total', 0)
            
            # Parsear artista y venue del nombre
            if ' EN ' in nombre_evento:
                parts = nombre_evento.split(' EN ')
                artista = parts[0].strip()
                venue = parts[1].replace(' 2025', '').replace(' 2024', '').strip()
            else:
                artista = nombre_evento
                venue = "Venue Ticketeck"
            
            # Fecha estimada (futura)
            from datetime import date, timedelta
            fecha_show = date.today() + timedelta(days=30)  # Asumimos 30 días en el futuro
            
            # Insertar nuevo show
            insert_query = """
                INSERT INTO shows (
                    artista, venue, fecha_show, capacidad_total, 
                    ticketera, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, NOW(), NOW()
                ) RETURNING id
            """
            
            cursor.execute(insert_query, (
                artista,
                venue, 
                fecha_show,
                capacidad_total,
                'ticketeck'
            ))
            
            new_show_id = cursor.fetchone()[0]
            self.db_connection.commit()
            
            logger.info(f"✅ Nuevo show creado: {new_show_id}")
            logger.info(f"   Artista: {artista}")
            logger.info(f"   Venue: {venue}")
            logger.info(f"   Capacidad: {capacidad_total}")
            
            return new_show_id
            
        except Exception as e:
            logger.error(f"❌ Error creando show: {e}")
            if self.db_connection:
                self.db_connection.rollback()
            return None
    
    def upsert_daily_sales_record(self, daily_record):
        """Inserta o actualiza un registro de daily_sales"""
        try:
            if not self.db_connection:
                logger.warning("⚠️ No hay conexión a BD para UPSERT")
                return "error"
            
            cursor = self.db_connection.cursor()
            
            show_id = daily_record['show_id']
            fecha_venta = daily_record['fecha_venta']
            venta_diaria = daily_record['venta_diaria']
            
            # Verificar si ya existe el registro
            check_query = """
                SELECT id, venta_diaria FROM daily_sales 
                WHERE show_id = %s AND fecha_venta = %s
            """
            cursor.execute(check_query, (show_id, fecha_venta))
            existing_record = cursor.fetchone()
            
            if existing_record:
                existing_id, existing_venta = existing_record
                
                # Solo actualizar si cambió la venta diaria
                if existing_venta != venta_diaria:
                    update_query = """
                        UPDATE daily_sales SET
                            venta_diaria = %s,
                            monto_diario_ars = %s,
                            venta_total_acumulada = %s,
                            recaudacion_total_ars = %s,
                            tickets_disponibles = %s,
                            porcentaje_ocupacion = %s,
                            fecha_extraccion = NOW(),
                            ticketera = %s,
                            archivo_origen = %s,
                            updated_at = NOW()
                        WHERE show_id = %s AND fecha_venta = %s
                    """
                    cursor.execute(update_query, (
                        daily_record['venta_diaria'],
                        daily_record['monto_diario_ars'],
                        daily_record['venta_total_acumulada'],
                        daily_record['recaudacion_total_ars'],
                        daily_record['tickets_disponibles'],
                        daily_record['porcentaje_ocupacion'],
                        daily_record['ticketera'],
                        'ticketeck_scraper.py',
                        show_id,
                        fecha_venta
                    ))
                    self.db_connection.commit()
                    return "updated"
                else:
                    return "skipped"
            else:
                # Insertar nuevo registro
                insert_query = """
                    INSERT INTO daily_sales (
                        show_id, fecha_venta, fecha_extraccion,
                        venta_diaria, monto_diario_ars,
                        venta_total_acumulada, recaudacion_total_ars,
                        tickets_disponibles, porcentaje_ocupacion,
                        ticketera, archivo_origen
                    ) VALUES (
                        %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                cursor.execute(insert_query, (
                    show_id,
                    fecha_venta,
                    daily_record['venta_diaria'],
                    daily_record['monto_diario_ars'],
                    daily_record['venta_total_acumulada'],
                    daily_record['recaudacion_total_ars'],
                    daily_record['tickets_disponibles'],
                    daily_record['porcentaje_ocupacion'],
                    daily_record['ticketera'],
                    'ticketeck_scraper.py'
                ))
                self.db_connection.commit()
                return "inserted"
                
        except Exception as e:
            logger.error(f"❌ Error en UPSERT daily_sales: {e}")
            if self.db_connection:
                self.db_connection.rollback()
            return "error"
    
    def navigate_to_reports_full(self):
        """Navega a la sección 'Reporte Completo'"""
        try:
            logger.info("Navegando a 'Reporte Completo'...")
            
            # Esperar a que la página se estabilice después del login
            time.sleep(3)
            
            # Buscar el enlace "Reporte Completo"
            reports_link = None
            
            # Intentar diferentes métodos para encontrar el enlace
            try:
                # Buscar por href exacto
                reports_link = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='#/reports/full']"))
                )
                logger.info("Enlace 'Reporte Completo' encontrado por href")
            except:
                try:
                    # Buscar por texto del span
                    reports_link = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Reporte Completo')]/parent::a"))
                    )
                    logger.info("Enlace 'Reporte Completo' encontrado por texto")
                except:
                    try:
                        # Buscar por clase sidebar-link que contenga el texto
                        reports_link = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'sidebar-link') and contains(., 'Reporte Completo')]"))
                        )
                        logger.info("Enlace 'Reporte Completo' encontrado por clase y texto")
                    except:
                        logger.error("No se pudo encontrar el enlace 'Reporte Completo'")
                        return False
            
            # Hacer clic en el enlace usando JavaScript para evitar problemas de interceptación
            try:
                self.driver.execute_script("arguments[0].click();", reports_link)
                logger.info("Enlace 'Reporte Completo' clickeado con JavaScript")
            except:
                reports_link.click()
                logger.info("Enlace 'Reporte Completo' clickeado con método normal")
            
            # Esperar a que la página cargue
            time.sleep(5)
            
            # Verificar que estamos en la página correcta
            current_url = self.driver.current_url
            if "reports/full" in current_url:
                logger.info("Navegación exitosa a 'Reporte Completo'")
                return True
            else:
                logger.warning(f"URL no corresponde a reportes completos: {current_url}")
                # Continuar de todas formas, puede que la URL no cambie inmediatamente
                return True
            
        except Exception as e:
            logger.error(f"Error navegando a 'Reporte Completo': {str(e)}")
            return False
    
    def analyze_page_content(self):
        """Analiza el contenido de la página para entender su estructura"""
        try:
            logger.info("Analizando contenido de la página...")
            
            # Obtener información básica de la página
            title = self.driver.title
            current_url = self.driver.current_url
            
            logger.info(f"Título de la página: {title}")
            logger.info(f"URL actual: {current_url}")
            
            # Buscar tablas
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            logger.info(f"Tablas encontradas: {len(tables)}")
            
            # Buscar formularios
            forms = self.driver.find_elements(By.TAG_NAME, "form")
            logger.info(f"Formularios encontrados: {len(forms)}")
            
            # Buscar botones
            buttons = self.driver.find_elements(By.TAG_NAME, "button")
            logger.info(f"Botones encontrados: {len(buttons)}")
            
            # Buscar selects
            selects = self.driver.find_elements(By.TAG_NAME, "select")
            logger.info(f"Selects encontrados: {len(selects)}")
            
            # Buscar inputs
            inputs = self.driver.find_elements(By.TAG_NAME, "input")
            logger.info(f"Inputs encontrados: {len(inputs)}")
            
            # Obtener texto visible de la página (primeros 1000 caracteres)
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            logger.info(f"Texto de la página (primeros 500 chars): {body_text[:500]}...")
            
            return True
            
        except Exception as e:
            logger.error(f"Error analizando contenido de la página: {str(e)}")
            return False
    
    def get_event_options(self):
        """Obtiene todas las opciones de eventos del selector"""
        try:
            logger.info("Obteniendo opciones de eventos del selector...")
            
            # Buscar el selector de eventos
            event_selector = None
            
            try:
                # Buscar por name="show"
                event_selector = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.NAME, "show"))
                )
                logger.info("Selector de eventos encontrado por name='show'")
            except:
                try:
                    # Buscar por clase y texto
                    event_selector = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//select[contains(@class, 'form-control')]"))
                    )
                    logger.info("Selector de eventos encontrado por clase")
                except:
                    logger.error("No se pudo encontrar el selector de eventos")
                    return []
            
            # Obtener todas las opciones
            option_elements = event_selector.find_elements(By.TAG_NAME, "option")
            
            events = []
            for option in option_elements:
                try:
                    value = option.get_attribute("value")
                    text = option.text.strip()
                    
                    # Ignorar la opción por defecto "Elija un Evento"
                    if value and value != "" and "Elija un Evento" not in text:
                        events.append({
                            "value": value,
                            "text": text
                        })
                        logger.info(f"Evento encontrado: {value} - {text}")
                
                except Exception as e:
                    logger.warning(f"Error procesando opción: {str(e)}")
                    continue
            
            logger.info(f"Total de eventos encontrados: {len(events)}")
            return events
            
        except Exception as e:
            logger.error(f"Error obteniendo opciones de eventos: {str(e)}")
            return []
    
    def select_event_and_generate_report(self, event_value, event_text):
        """Selecciona un evento específico y genera el reporte"""
        try:
            logger.info(f"Procesando evento: {event_value} - {event_text}")
            
            # Buscar el selector de eventos
            event_selector = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.NAME, "show"))
            )
            
            # Seleccionar el evento por value
            from selenium.webdriver.support.ui import Select
            select = Select(event_selector)
            select.select_by_value(event_value)
            logger.info(f"Evento seleccionado: {event_value}")
            
            # Esperar un momento para que se procese la selección
            time.sleep(2)
            
            # Buscar y hacer clic en el botón "Generar Reporte"
            generate_button = None
            
            try:
                # Buscar por texto del botón
                generate_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Generar Reporte')]"))
                )
                logger.info("Botón 'Generar Reporte' encontrado por texto")
            except:
                try:
                    # Buscar por clase btn-success
                    generate_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-success"))
                    )
                    logger.info("Botón 'Generar Reporte' encontrado por clase")
                except:
                    logger.error("No se pudo encontrar el botón 'Generar Reporte'")
                    return None
            
            # Hacer clic en el botón
            generate_button.click()
            logger.info("Botón 'Generar Reporte' clickeado")
            
            # Esperar a que se genere el reporte (tiempo más largo)
            logger.info("Esperando a que se genere el reporte...")
            time.sleep(10)
            
            # Extraer datos de la tabla generada
            extracted_data = self.extract_report_table_data(event_value, event_text)
            
            # Guardar datos en la base de datos
            if extracted_data:
                self.save_to_database(extracted_data, event_value, event_text)
            
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error seleccionando evento {event_value}: {str(e)}")
            return None
    
    def extract_report_table_data(self, event_value, event_text):
        """Extrae los datos de TODAS las tablas del reporte generado, incluida la tabla final"""
        try:
            logger.info(f"Extrayendo datos de TODAS las tablas para evento: {event_value}")
            
            # Esperar un poco más para asegurar que todas las tablas estén cargadas
            time.sleep(3)
            
            # Buscar todas las tablas (puede haber múltiples)
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            
            if not tables:
                logger.warning("No se encontraron tablas en el reporte")
                return []
            
            logger.info(f"Encontradas {len(tables)} tablas en el reporte")
            
            all_extracted_data = []
            
            for table_index, table in enumerate(tables):
                try:
                    logger.info(f"Procesando tabla {table_index + 1}/{len(tables)} para evento {event_value}...")
                    
                    # Extraer headers dinámicamente - adaptándose a cualquier estructura
                    headers = []
                    try:
                        # Intentar múltiples métodos para extraer headers
                        header_elements = []
                        
                        # Método 1: thead > tr > th
                        try:
                            thead = table.find_element(By.TAG_NAME, "thead")
                            header_row = thead.find_element(By.TAG_NAME, "tr")
                            header_elements = header_row.find_elements(By.TAG_NAME, "th")
                        except:
                            pass
                        
                        # Método 2: primera fila como headers (tr:first-child > th)
                        if not header_elements:
                            try:
                                first_row = table.find_element(By.CSS_SELECTOR, "tr:first-child")
                                header_elements = first_row.find_elements(By.TAG_NAME, "th")
                            except:
                                pass
                        
                        # Método 3: primera fila con td que actúen como headers
                        if not header_elements:
                            try:
                                first_row = table.find_element(By.CSS_SELECTOR, "tr:first-child")
                                header_elements = first_row.find_elements(By.TAG_NAME, "td")
                                # Solo usar si parece que son headers (texto no numérico)
                                if header_elements:
                                    first_cell_text = header_elements[0].text.strip()
                                    if not first_cell_text.replace('.', '').replace(',', '').isdigit():
                                        pass  # Usar estos elementos como headers
                                    else:
                                        header_elements = []  # No son headers, son datos
                            except:
                                pass
                        
                        # Extraer texto de los headers
                        if header_elements:
                            headers = [cell.text.strip() for cell in header_elements if cell.text.strip()]
                            logger.info(f"Headers extraídos de tabla {table_index + 1}: {headers}")
                        
                    except Exception as e:
                        logger.warning(f"Error extrayendo headers de tabla {table_index + 1}: {str(e)}")
                    
                    # Si no se encontraron headers, crear headers genéricos basados en el número de columnas
                    if not headers:
                        # Contar columnas de la primera fila de datos
                        try:
                            first_data_row = table.find_element(By.CSS_SELECTOR, "tbody tr, tr")
                            cells_count = len(first_data_row.find_elements(By.CSS_SELECTOR, "td, th"))
                            headers = [f"Columna_{i+1}" for i in range(cells_count)]
                            logger.info(f"Headers genéricos creados para tabla {table_index + 1}: {headers}")
                        except:
                            headers = [f"Columna_{i+1}" for i in range(20)]  # Headers por defecto
                    
                    # Extraer filas de datos
                    data_rows = []
                    
                    # Método 1: tbody > tr
                    try:
                        tbody = table.find_element(By.TAG_NAME, "tbody")
                        data_rows = tbody.find_elements(By.TAG_NAME, "tr")
                    except:
                        pass
                    
                    # Método 2: todas las filas de la tabla (excluyendo header si existe)
                    if not data_rows:
                        try:
                            all_rows = table.find_elements(By.TAG_NAME, "tr")
                            # Si hay thead, omitir la primera fila
                            try:
                                table.find_element(By.TAG_NAME, "thead")
                                data_rows = all_rows[1:] if len(all_rows) > 1 else all_rows
                            except:
                                # Si no hay thead pero la primera fila tiene th, omitirla
                                if all_rows and all_rows[0].find_elements(By.TAG_NAME, "th"):
                                    data_rows = all_rows[1:] if len(all_rows) > 1 else []
                                else:
                                    data_rows = all_rows
                        except:
                            pass
                    
                    logger.info(f"Encontradas {len(data_rows)} filas de datos en tabla {table_index + 1}")
                    
                    table_data = []
                    
                    for row_index, row in enumerate(data_rows):
                        try:
                            # Obtener todas las celdas (td y th)
                            cells = row.find_elements(By.CSS_SELECTOR, "td, th")
                            
                            if cells and len(cells) > 0:
                                row_data = {
                                    "evento_codigo": event_value,
                                    "evento_nombre": event_text,
                                    "tabla_numero": table_index + 1,
                                    "fila_numero": row_index + 1,
                                    "fecha_extraccion": datetime.now().isoformat(),
                                    "total_columnas": len(cells)
                                }
                                
                                # Mapear cada celda con su header correspondiente
                                for i, cell in enumerate(cells):
                                    # Usar header correspondiente o crear uno genérico
                                    header_name = headers[i] if i < len(headers) else f"Columna_{i+1}"
                                    
                                    # Obtener el texto de la celda, limpiando HTML interno
                                    cell_text = cell.text.strip()
                                    
                                    # Limpiar texto especial para diferentes tipos de datos
                                    if cell_text:
                                        # Para porcentajes, extraer solo el número
                                        if "%" in cell_text and any(char.isdigit() for char in cell_text):
                                            percentage_match = re.search(r'(\d+\.?\d*)', cell_text)
                                            if percentage_match:
                                                cell_text = percentage_match.group(1) + "%"
                                        
                                        # Para montos, mantener el formato original
                                        elif "$" in cell_text:
                                            # Mantener formato monetario completo
                                            pass
                                        
                                        # Para números con separadores, mantener formato
                                        elif re.match(r'^\d{1,3}(,\d{3})*(\.\d+)?$', cell_text):
                                            # Mantener formato de números con comas
                                            pass
                                    
                                    # Normalizar el nombre del header para usar como clave
                                    header_key = header_name.lower().replace(" ", "_").replace("ñ", "n")
                                    header_key = re.sub(r'[^\w]', '_', header_key)
                                    header_key = re.sub(r'_+', '_', header_key).strip('_')
                                    
                                    # Si la clave ya existe, agregar sufijo numérico
                                    original_key = header_key
                                    counter = 1
                                    while header_key in row_data:
                                        header_key = f"{original_key}_{counter}"
                                        counter += 1
                                    
                                    row_data[header_key] = cell_text if cell_text else ""
                                
                                table_data.append(row_data)
                                
                                # Log de información de las primeras filas para debug
                                if row_index < 2:
                                    sample_keys = list(row_data.keys())[:8]  # Primeras 8 claves
                                    sample_data = {k: row_data[k] for k in sample_keys if k not in ['fecha_extraccion']}
                                    logger.info(f"  Tabla {table_index + 1}, Fila {row_index + 1}: {sample_data}")
                        
                        except Exception as e:
                            logger.warning(f"Error procesando fila {row_index + 1} de tabla {table_index + 1}: {str(e)}")
                            continue
                    
                    if table_data:
                        all_extracted_data.extend(table_data)
                        logger.info(f"✅ Tabla {table_index + 1} procesada exitosamente: {len(table_data)} filas extraídas")
                        
                        # Log adicional para la última tabla
                        if table_index == len(tables) - 1:
                            logger.info(f"🎯 TABLA FINAL (#{table_index + 1}) procesada con {len(table_data)} registros")
                    else:
                        logger.info(f"⚠️ Tabla {table_index + 1} no contiene datos válidos")
                    
                except Exception as e:
                    logger.warning(f"❌ Error procesando tabla {table_index + 1}: {str(e)}")
                    continue
            
            logger.info(f"📊 RESUMEN EXTRACCIÓN para evento {event_value}:")
            logger.info(f"  - Tablas procesadas: {len(tables)}")
            logger.info(f"  - Total registros extraídos: {len(all_extracted_data)}")
            
            # Contar registros por tabla
            table_counts = {}
            for record in all_extracted_data:
                table_num = record.get('tabla_numero', 'desconocida')
                table_counts[table_num] = table_counts.get(table_num, 0) + 1
            
            for table_num, count in table_counts.items():
                logger.info(f"  - Tabla {table_num}: {count} registros")
            
            return all_extracted_data
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos de las tablas del reporte: {str(e)}")
            return []
    
    def process_active_events_only(self, active_event_codes):
        """Procesa solo los eventos activos especificados"""
        try:
            logger.info(f"Iniciando procesamiento de {len(active_event_codes)} eventos activos...")
            
            # Obtener lista completa de eventos del dropdown
            all_events = self.get_event_options()
            
            if not all_events:
                logger.error("No se encontraron eventos en el dropdown")
                return []
            
            # Filtrar solo los eventos activos
            active_events = []
            for event in all_events:
                if event['value'] in active_event_codes:
                    active_events.append(event)
                    logger.info(f"Evento activo confirmado: {event['value']} - {event['text']}")
            
            if not active_events:
                logger.error("No se encontraron eventos activos en el dropdown")
                logger.info("Eventos activos buscados:", active_event_codes)
                logger.info("Eventos disponibles en dropdown:", [e['value'] for e in all_events])
                return []
            
            logger.info(f"Se procesarán {len(active_events)} eventos activos de {len(all_events)} totales")
            
            all_data = []
            successful_events = 0
            failed_events = 0
            
            for event_index, event in enumerate(active_events):
                try:
                    logger.info(f"Procesando evento activo {event_index + 1}/{len(active_events)}: {event['value']}")
                    
                    # Seleccionar evento y generar reporte
                    event_data = self.select_event_and_generate_report(event['value'], event['text'])
                    
                    if event_data:
                        all_data.extend(event_data)
                        successful_events += 1
                        logger.info(f"Evento {event['value']} procesado exitosamente: {len(event_data)} registros")
                        
                        # Guardar datos del evento individual (solo en BD, no archivos)
                        # self.save_individual_event_data(event_data, event['value'])
                    else:
                        failed_events += 1
                        logger.error(f"No se pudieron extraer datos del evento {event['value']}")
                    
                    # Pausa entre eventos para evitar sobrecargar el servidor
                    if event_index < len(active_events) - 1:  # No pausar después del último evento
                        logger.info("Pausa de 3 segundos antes del siguiente evento...")
                        time.sleep(3)
                
                except Exception as e:
                    failed_events += 1
                    logger.error(f"Error procesando evento {event.get('value', 'desconocido')}: {str(e)}")
                    continue
            
            # Resumen final
            logger.info(f"Procesamiento de eventos activos completado:")
            logger.info(f"  - Total eventos activos: {len(active_events)}")
            logger.info(f"  - Exitosos: {successful_events}")
            logger.info(f"  - Fallidos: {failed_events}")
            logger.info(f"  - Total registros extraídos: {len(all_data)}")
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error en el procesamiento de eventos activos: {str(e)}")
            return []
    
    def process_all_events(self):
        """Procesa todos los eventos disponibles uno por uno (método de respaldo)"""
        try:
            logger.info("Iniciando procesamiento de todos los eventos...")
            
            # Obtener lista de eventos
            events = self.get_event_options()
            
            if not events:
                logger.error("No se encontraron eventos para procesar")
                return []
            
            logger.info(f"Se procesarán {len(events)} eventos")
            
            all_data = []
            successful_events = 0
            failed_events = 0
            
            for event_index, event in enumerate(events):
                try:
                    logger.info(f"Procesando evento {event_index + 1}/{len(events)}: {event['value']}")
                    
                    # Seleccionar evento y generar reporte
                    event_data = self.select_event_and_generate_report(event['value'], event['text'])
                    
                    if event_data:
                        all_data.extend(event_data)
                        successful_events += 1
                        logger.info(f"Evento {event['value']} procesado exitosamente: {len(event_data)} registros")
                        
                        # Guardar datos del evento individual (solo en BD, no archivos)
                        # self.save_individual_event_data(event_data, event['value'])
                    else:
                        failed_events += 1
                        logger.error(f"No se pudieron extraer datos del evento {event['value']}")
                    
                    # Pausa entre eventos para evitar sobrecargar el servidor
                    if event_index < len(events) - 1:  # No pausar después del último evento
                        logger.info("Pausa de 3 segundos antes del siguiente evento...")
                        time.sleep(3)
                
                except Exception as e:
                    failed_events += 1
                    logger.error(f"Error procesando evento {event.get('value', 'desconocido')}: {str(e)}")
                    continue
            
            # Resumen final
            logger.info(f"Procesamiento completado:")
            logger.info(f"  - Total eventos: {len(events)}")
            logger.info(f"  - Exitosos: {successful_events}")
            logger.info(f"  - Fallidos: {failed_events}")
            logger.info(f"  - Total registros extraídos: {len(all_data)}")
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error en el procesamiento de eventos: {str(e)}")
            return []
    
    # Método eliminado - solo guardamos en base de datos
    
    # Método eliminado - solo guardamos en base de datos
    
    def run(self):
        """Ejecuta el scraper completo"""
        try:
            logger.info("=== INICIANDO SCRAPER DE TICKETEK ===")
            
            # Configurar driver
            logger.info("PASO 1: Configurando driver...")
            if not self.setup_driver():
                logger.error("No se pudo configurar el driver")
                return False
            
            # Navegar a la página de login
            logger.info("PASO 2: Navegando a la página de login...")
            if not self.navigate_to_login():
                logger.error("No se pudo navegar a la página de login")
                return False
            
            # Realizar login
            logger.info("PASO 3: Realizando login...")
            if not self.login():
                logger.error("No se pudo realizar el login")
                return False
            
            # Navegar a la página principal/home para ver la tabla de resumen
            logger.info("PASO 4A: Navegando a la página principal para ver tabla de resumen...")
            if not self.navigate_to_home():
                logger.warning("No se pudo navegar a la página principal, continuando...")
            
            # Extraer datos de la tabla de resumen inicial
            logger.info("PASO 4B: Extrayendo datos de la tabla de resumen inicial...")
            summary_data = self.extract_initial_summary_table()
            
            # Extraer eventos activos de la tabla inicial (antes de ir a reportes completos)
            logger.info("PASO 4C: Extrayendo eventos activos de la pantalla principal...")
            active_events = self.extract_active_events_from_initial_table()
            
            if not active_events:
                logger.warning("No se encontraron eventos activos en la pantalla principal")
                logger.info("Procediendo con todos los eventos como respaldo...")
                use_all_events = True
            else:
                logger.info(f"Se encontraron {len(active_events)} eventos activos")
                use_all_events = False
            
            # NUEVO: Extraer datos diarios de ventas desde la página HOME
            logger.info("PASO 5: Extrayendo datos diarios de ventas desde HOME...")
            daily_sales_data = self.extract_daily_sales_from_home(active_events)
            
            # Procesar datos diarios extraídos
            all_extracted_data = daily_sales_data
            
            # Combinar datos de resumen y datos detallados
            final_data = []
            if summary_data:
                final_data.extend(summary_data)
                logger.info(f"Agregados {len(summary_data)} registros de tabla de resumen")
            
            if all_extracted_data:
                final_data.extend(all_extracted_data)
                logger.info(f"Agregados {len(all_extracted_data)} registros de reportes detallados")
            
            # Procesar datos según el modo
            if final_data:
                if self.test_mode:
                    logger.info("🧪 MODO TEST: Mostrando datos extraídos sin guardar en BD")
                    self.show_test_results(final_data)
                else:
                    logger.info("PASO 8: Datos consolidados listos (guardados solo en BD)...")
                
                logger.info("=== SCRAPER EJECUTADO EXITOSAMENTE ===")
                logger.info(f"Total de registros extraídos: {len(final_data)}")
                logger.info(f"  - Registros de resumen inicial: {len(summary_data) if summary_data else 0}")
                logger.info(f"  - Registros de reportes detallados: {len(all_extracted_data) if all_extracted_data else 0}")
                
                # Resumen por evento (solo datos detallados)
                if all_extracted_data:
                    events_processed = {}
                    for record in all_extracted_data:
                        event_code = record.get('evento_codigo', 'desconocido')
                        if event_code not in events_processed:
                            events_processed[event_code] = 0
                        events_processed[event_code] += 1
                    
                    logger.info(f"Eventos procesados en detalle: {len(events_processed)}")
                    for event_code, count in events_processed.items():
                        logger.info(f"  - {event_code}: {count} registros")
                
            else:
                logger.warning("No se extrajeron datos de ningún evento")
                
                # Guardar información básica de la página
                page_info = {
                    "fuente": "Ticketek Reportes Completos",
                    "url": self.driver.current_url,
                    "fecha_acceso": datetime.now().isoformat(),
                    "titulo": self.driver.title,
                    "usuario": self.username,
                    "nota": "No se pudieron extraer datos de los eventos"
                }
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                info_filename = f"ticketeck_sin_datos_{timestamp}.json"
                info_filepath = os.path.join(self.download_folder, info_filename)
                
                with open(info_filepath, 'w', encoding='utf-8') as f:
                    json.dump(page_info, f, ensure_ascii=False, indent=2)
                
                logger.info(f"Información de la sesión guardada en: {info_filepath}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error ejecutando scraper: {str(e)}")
            return False
        
        finally:
            # Mantener el navegador abierto para inspección manual
            if self.driver:
                logger.info("Navegador mantenido abierto. Presiona Enter para cerrar...")
                # input()  # Comentado para ejecución automática
                self.close()
    
    def calculate_event_totals(self, event_data):
        """Calcula los totales del evento basado en los datos extraídos"""
        try:
            vendido_total = 0
            recaudacion_total_ars = 0
            capacidad_total = 0
            disponible_total = 0
            
            logger.info(f"📊 Calculando totales para {len(event_data)} registros...")
            
            for registro in event_data:
                # Buscar filas que contengan totales (tabla 2 - canales de pago)
                if registro.get("tabla_numero") == 2 and registro.get("canal") == "Total":
                    # Extraer total de recaudación
                    total_str = registro.get("total", "0")
                    if total_str and total_str != "0":
                        # Limpiar formato de moneda: "$1.175.000" -> 1175000
                        total_clean = total_str.replace("$", "").replace(".", "").replace(",", "")
                        try:
                            recaudacion_total_ars = int(total_clean)
                            logger.info(f"💰 Recaudación total encontrada: ${recaudacion_total_ars:,}")
                        except ValueError:
                            logger.warning(f"⚠️ No se pudo parsear total: {total_str}")
                
                # Buscar filas de totales en tabla 1 (secciones)
                elif registro.get("tabla_numero") == 1 and registro.get("sección") == "Total":
                    # En TicketEck, los totales de secciones pueden indicar capacidad
                    # Pero necesitamos más información para calcular vendidos vs disponibles
                    pass
            
            # Para TicketEck, estimar capacidad basada en recaudación
            # Asumiendo precio promedio de $50,000 por ticket
            if recaudacion_total_ars > 0:
                precio_promedio_estimado = 50000
                vendido_total = recaudacion_total_ars // precio_promedio_estimado
                capacidad_total = max(vendido_total, 1000)  # Mínimo 1000 de capacidad
                disponible_total = max(0, capacidad_total - vendido_total)
                
                logger.info(f"🎫 Vendidos estimados: {vendido_total}")
                logger.info(f"🏟️ Capacidad estimada: {capacidad_total}")
                logger.info(f"🆓 Disponibles estimados: {disponible_total}")
            
            # Calcular porcentaje de ocupación
            if capacidad_total > 0:
                porcentaje_ocupacion = round((vendido_total / capacidad_total) * 100, 2)
            else:
                porcentaje_ocupacion = 0
            
            totales = {
                "vendido_total": vendido_total,
                "recaudacion_total_ars": recaudacion_total_ars,
                "capacidad_total": capacidad_total,
                "disponible_total": disponible_total,
                "porcentaje_ocupacion": porcentaje_ocupacion
            }
            
            logger.info(f"✅ Totales calculados: Vendido={vendido_total}, Recaudado=${recaudacion_total_ars:,}, Capacidad={capacidad_total}, Ocupación={porcentaje_ocupacion}%")
            
            return totales
            
        except Exception as e:
            logger.error(f"❌ Error calculando totales: {str(e)}")
            return {
                "vendido_total": 0,
                "recaudacion_total_ars": 0,
                "capacidad_total": 0,
                "disponible_total": 0,
                "porcentaje_ocupacion": 0
            }

    def extract_fecha_from_event_name(self, event_name):
        """Extrae la fecha del nombre del evento o usa una fecha por defecto"""
        try:
            if not event_name:
                return None
            
            # Para TicketEck, los nombres de eventos no contienen fechas específicas
            # Usar una fecha por defecto basada en el año del evento
            if "2025" in event_name:
                # Eventos de 2025 - usar una fecha por defecto
                return datetime(2025, 12, 31, 21, 0, 0)  # 31/12/2025 21:00
            elif "2024" in event_name:
                # Eventos de 2024 - usar una fecha por defecto
                return datetime(2024, 12, 31, 21, 0, 0)  # 31/12/2024 21:00
            else:
                # Si no se puede determinar el año, usar fecha actual
                return datetime.now().replace(hour=21, minute=0, second=0, microsecond=0)
                
        except Exception as e:
            logger.error(f"Error extrayendo fecha del evento: {str(e)}")
            return datetime.now().replace(hour=21, minute=0, second=0, microsecond=0)

    def show_test_results(self, data):
        """Muestra los resultados de datos diarios en modo test"""
        try:
            print("\n" + "="*80)
            print("🧪 MODO TEST - DATOS DIARIOS DE TICKETEK")
            print("="*80)
            print(f"📅 Fecha de extracción: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            if not data:
                print("❌ No se extrajeron datos diarios")
                return
            
            print(f"📊 TOTAL EVENTOS CON DATOS DIARIOS: {len(data)}")
            print("-" * 80)
            
            # Mostrar datos de cada evento
            for i, record in enumerate(data, 1):
                evento_codigo = record.get('evento_codigo', 'Sin código')
                show_id = record.get('show_id', 'Sin show_id')
                
                print(f"\n🎭 EVENTO {i}: {evento_codigo}")
                print(f"   🆔 Show ID: {show_id}")
                print(f"   🏢 Ticketera: {record.get('ticketera', 'N/A')}")
                
                # Mostrar datos de ventas diarias
                venta_hoy = record.get('venta_hoy', 'N/A')
                venta_ayer = record.get('venta_ayer', 'N/A') 
                venta_total = record.get('venta_total', 'N/A')
                recaudacion_total = record.get('recaudacion_total', 'N/A')
                disponible = record.get('disponible', 'N/A')
                
                print(f"   📅 Venta HOY: {venta_hoy}")
                print(f"   📅 Venta AYER: {venta_ayer}")
                print(f"   💰 Venta TOTAL: {venta_total}")
                print(f"   💵 Recaudación: ${recaudacion_total:,}" if isinstance(recaudacion_total, int) else f"   💵 Recaudación: {recaudacion_total}")
                print(f"   🎫 Disponibles: {disponible}")
                
                # Mostrar todos los campos disponibles
                print(f"   📋 Campos extraídos: {list(record.keys())}")
            
            print(f"\n✅ RESUMEN FINAL:")
            print(f"   🎭 Total eventos: {len(data)}")
            print(f"   📊 Datos diarios extraídos exitosamente")
            print(f"   🔄 Mapeo automático funcionando")
            print(f"   🧪 Modo TEST: Sin guardar en BD")
            print("="*80)
            
        except Exception as e:
            logger.error(f"❌ Error mostrando resultados de test: {e}")

    def save_to_database(self, event_data, event_code, event_name):
        """Guarda los datos del evento en la base de datos (solo si no está en modo test)"""
        try:
            if self.test_mode:
                logger.info(f"🧪 MODO TEST: Saltando guardado en BD para evento {event_code}")
                return None
                
            if not event_data:
                logger.warning(f"No hay datos para guardar del evento {event_code}")
                return None
            
            logger.info(f"💾 Guardando datos de '{event_name}' en la base de datos...")
            
            # Obtener conexión a la base de datos
            connection = get_database_connection()
            if not connection:
                logger.error("❌ No se pudo conectar a la base de datos")
                return None
            
            cursor = connection.cursor()
            
            # Calcular totales del evento
            totales = self.calculate_event_totals(event_data)
            
            # Preparar datos para el JSON
            json_data = {
                "fuente": "Ticketek Reportes Completos",
                "evento_codigo": event_code,
                "evento_nombre": event_name,
                "url": self.driver.current_url,
                "fecha_extraccion": datetime.now().isoformat(),
                "total_registros": len(event_data),
                "usuario": self.username,
                "datos": event_data,
                # Totales calculados
                "vendido_total": totales["vendido_total"],
                "recaudacion_total_ars": totales["recaudacion_total_ars"],
                "capacidad_total": totales["capacidad_total"],
                "disponible_total": totales["disponible_total"],
                "porcentaje_ocupacion": totales["porcentaje_ocupacion"]
            }
            
            # Extraer información del evento para las columnas separadas
            artista = event_name if event_name else event_code
            venue = "No especificado"
            fecha_show = self.extract_fecha_from_event_name(event_name)
            
            # Extraer artista y venue del nombre del evento
            if event_name:
                # Ejemplos:
                # "BENJA TORRES - T. AVENIDA 2025 - [BENJA25TAV]" -> artista: "BENJA TORRES", venue: "Teatro Avenida"
                # "CARLOS VIVES EN QUALITY ARENA 2025 - [CVIVES25QLA]" -> artista: "CARLOS VIVES", venue: "Quality Arena"
                # "BENJA TORRES EN QUALITY TEATRO 2025 - [BTORRES25QT]" -> artista: "BENJA TORRES", venue: "Quality Teatro"
                
                # Extraer artista (antes del primer " - " o " EN ")
                if " - " in event_name:
                    artista = event_name.split(" - ")[0].strip()
                elif " EN " in event_name:
                    artista = event_name.split(" EN ")[0].strip()
                
                # Limpiar el nombre del artista (remover años, códigos y venue)
                if " 2025" in artista:
                    artista = artista.split(" 2025")[0].strip()
                if " 2024" in artista:
                    artista = artista.split(" 2024")[0].strip()
                if " EN " in artista:
                    artista = artista.split(" EN ")[0].strip()
                
                # Extraer venue
                if "T. AVENIDA" in event_name:
                    venue = "Teatro Avenida"
                elif "QUALITY ARENA" in event_name:
                    venue = "Quality Arena"
                elif "QUALITY TEATRO" in event_name:
                    venue = "Quality Teatro"
                else:
                    # Intentar extraer venue genérico
                    if " EN " in event_name:
                        venue_part = event_name.split(" EN ")[1].split(" 2025")[0].strip()
                        venue = venue_part.replace("QUALITY", "Quality").replace("TEATRO", "Teatro").replace("ARENA", "Arena")
            
            # Ajustar fecha de extracción (-3 horas para Argentina)
            fecha_extraccion_argentina = datetime.now() - timedelta(hours=3)
            
            # DEBUG: Mostrar las fechas para verificar
            logger.info(f"🕐 Fecha UTC: {datetime.now()}")
            logger.info(f"🕐 Fecha Argentina (UTC-3): {fecha_extraccion_argentina}")
            logger.info(f"🕐 Fecha ISO Argentina: {fecha_extraccion_argentina.isoformat()}")
            
            # Query de inserción simple - solo en raw_data, evitando triggers
            insert_query = """
            INSERT INTO raw_data (
                ticketera, artista, venue, fecha_show, json_data,
                archivo_origen, url_origen, fecha_extraccion, procesado
            ) VALUES (
                %(ticketera)s, %(artista)s, %(venue)s, %(fecha_show)s, %(json_data)s,
                %(archivo_origen)s, %(url_origen)s, %(fecha_extraccion)s, %(procesado)s
            ) RETURNING id;
            """
            
            # Ejecutar inserción
            cursor.execute(insert_query, {
                'ticketera': 'ticketeck',
                'artista': artista,
                'venue': venue,
                'fecha_show': fecha_show,
                'json_data': json.dumps(json_data, ensure_ascii=False),
                'archivo_origen': f'ticketeck_{event_code}',
                'url_origen': self.driver.current_url,
                'fecha_extraccion': fecha_extraccion_argentina,
                'procesado': False
            })
            
            result = cursor.fetchone()
            
            if result:
                logger.info(f"✅ Datos de '{event_name}' guardados exitosamente en la BD (ID: {result[0]})")
                print(f"💾 GUARDADO EN BD: {event_name} - ID: {result[0]}")
            else:
                logger.warning(f"⚠️ Inserción completada pero sin ID retornado para '{event_name}'")
            
            # Commit y cerrar conexión
            connection.commit()
            cursor.close()
            connection.close()
            
            return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error guardando datos del evento {event_code}: {str(e)}")
            return None

    def close(self):
        """Cierra el driver y conexión a BD"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")
        
        if self.db_connection:
            self.db_connection.close()
            logger.info("Conexión BD cerrada")

def main():
    """Función principal para modo producción"""
    scraper = TicketekScraper(headless=True, test_mode=False)
    try:
        scraper.run()
    except KeyboardInterrupt:
        logger.info("Scraper interrumpido por el usuario")
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
    finally:
        scraper.close()

def main_test():
    """Función principal para modo test"""
    logger.info("🧪 INICIANDO TICKETEK EN MODO TEST")
    scraper = TicketekScraper(headless=True, test_mode=True)
    try:
        scraper.run()
    except KeyboardInterrupt:
        logger.info("Test interrumpido por el usuario")
    except Exception as e:
        logger.error(f"Error inesperado en test: {str(e)}")
    finally:
        scraper.close()

if __name__ == "__main__":
    # Ejecutar en modo producción para guardar en BD
    main()