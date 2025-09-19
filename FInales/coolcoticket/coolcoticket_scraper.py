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

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CoolcoTicketScraper:
    def __init__(self, headless=False):
        """
        Inicializa el scraper de Coolco Ticket
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless
        """
        self.driver = None
        self.headless = headless
        self.download_folder = "jsoncoolcoticket"
        
        # Credenciales de login
        self.username = "CamilaHalfon"
        self.password = "1234"
        self.login_url = "https://ticketing.coolco.io/backoffice/login"
        
        # Crear carpeta de descarga si no existe
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)
            logger.info(f"Carpeta creada: {self.download_folder}")
        else:
            logger.info(f"Carpeta ya existe: {self.download_folder}")
    
    def setup_driver(self):
        """Configura el driver de Chrome con las opciones necesarias"""
        try:
            logger.info("Configurando driver de Chrome...")
            
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument("--headless")
                logger.info("Modo headless activado")
            
            # Opciones adicionales para estabilidad
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Configurar descarga de archivos
            prefs = {
                "download.default_directory": os.path.abspath(self.download_folder),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            # Instalar ChromeDriver automáticamente con configuración específica
            try:
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as e:
                logger.warning(f"Error con ChromeDriverManager: {e}")
                logger.info("Intentando usar ChromeDriver del sistema...")
                # Intentar usar ChromeDriver del sistema
                self.driver = webdriver.Chrome(options=chrome_options)
            
            # Configurar timeouts
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            logger.info("Driver de Chrome configurado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error configurando el driver: {str(e)}")
            return False
    
    def navigate_to_website(self, url):
        """Navega al sitio web especificado"""
        try:
            logger.info(f"Navegando a: {url}")
            self.driver.get(url)
            time.sleep(3)
            
            # Verificar que la página se cargó correctamente
            if "error" in self.driver.title.lower() or "404" in self.driver.title.lower():
                logger.error(f"Error en la página: {self.driver.title}")
                return False
            
            logger.info(f"Página cargada exitosamente: {self.driver.title}")
            return True
            
        except Exception as e:
            logger.error(f"Error navegando al sitio web: {str(e)}")
            return False
    
    def login(self):
        """Realiza el login en el sistema Coolco Ticket"""
        try:
            logger.info("Iniciando proceso de login...")
            
            # Navegar a la página de login
            logger.info(f"Navegando a: {self.login_url}")
            self.driver.get(self.login_url)
            time.sleep(3)
            
            # PASO 1: Ingresar usuario
            logger.info("PASO 1: Ingresando usuario...")
            
            # Buscar el campo de usuario
            try:
                user_field = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "login"))
                )
                logger.info("Campo de usuario encontrado")
            except:
                logger.error("No se encontró el campo de usuario")
                return False
            
            # Limpiar y escribir el usuario
            user_field.clear()
            user_field.send_keys(self.username)
            logger.info(f"Usuario ingresado: {self.username}")
            
            # Buscar y hacer clic en el botón "Continuar"
            try:
                continue_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "btnLogin"))
                )
                continue_button.click()
                logger.info("Botón 'Continuar' clickeado")
                time.sleep(3)
            except:
                logger.error("No se encontró el botón 'Continuar'")
                return False
            
            # PASO 2: Ingresar contraseña
            logger.info("PASO 2: Ingresando contraseña...")
            
            # Esperar a que aparezca el campo de contraseña
            try:
                password_field = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "password"))
                )
                logger.info("Campo de contraseña encontrado")
            except:
                logger.error("No se encontró el campo de contraseña")
                return False
            
            # Limpiar y escribir la contraseña
            password_field.clear()
            password_field.send_keys(self.password)
            logger.info("Contraseña ingresada")
            
            # Buscar y hacer clic en el botón "Acceder"
            try:
                access_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "btnLoginPasswd"))
                )
                access_button.click()
                logger.info("Botón 'Acceder' clickeado")
                time.sleep(5)
            except:
                logger.error("No se encontró el botón 'Acceder'")
                return False
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            logger.info(f"URL actual después del login: {current_url}")
            
            # Si no estamos más en la página de login, el login fue exitoso
            if "login" not in current_url.lower():
                logger.info("Login exitoso")
                return True
            else:
                # Verificar si hay mensajes de error
                try:
                    error_container = self.driver.find_element(By.ID, "passwordErrorContainer")
                    if error_container.is_displayed():
                        error_msg = self.driver.find_element(By.ID, "passwordErrorMsg").text
                        logger.error(f"Error de login: {error_msg}")
                        return False
                except:
                    pass
                
                logger.warning("El login puede no haber sido exitoso")
                return False
                
        except Exception as e:
            logger.error(f"Error durante el login: {str(e)}")
            return False
    
    def click_personalizado_button(self):
        """Hace clic en el botón 'Personalizado' después del login"""
        try:
            logger.info("Buscando y haciendo clic en el botón 'Personalizado'...")
            
            # Buscar el botón "Personalizado" por ID
            try:
                personalizado_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "button-customdate"))
                )
                logger.info("Botón 'Personalizado' encontrado")
            except:
                logger.error("No se encontró el botón 'Personalizado' por ID")
                
                # Intentar buscar por texto como respaldo
                try:
                    personalizado_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Personalizado')]"))
                    )
                    logger.info("Botón 'Personalizado' encontrado por texto")
                except:
                    logger.error("No se encontró el botón 'Personalizado'")
                    return False
            
            # Hacer clic en el botón
            personalizado_button.click()
            logger.info("Botón 'Personalizado' clickeado exitosamente")
            
            # Esperar a que se cargue el contenido personalizado
            time.sleep(3)
            
            return True
            
        except Exception as e:
            logger.error(f"Error haciendo clic en el botón 'Personalizado': {str(e)}")
            return False
    
    def configure_custom_dates(self):
        """Configura las fechas personalizadas: desde 1 enero 2025 hasta 10 días después de hoy"""
        try:
            logger.info("Configurando fechas personalizadas...")
            
            # Calcular fechas
            fecha_inicio = "01/01/2025"  # 1 de enero de 2025
            fecha_fin = (datetime.now() + timedelta(days=10)).strftime("%d/%m/%Y")  # 10 días después de hoy
            
            logger.info(f"Fecha inicio: {fecha_inicio}")
            logger.info(f"Fecha fin: {fecha_fin}")
            
            # Buscar y configurar campo de fecha inicio
            try:
                fecha_inicio_field = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "dateFrom"))
                )
                fecha_inicio_field.clear()
                fecha_inicio_field.send_keys(fecha_inicio)
                logger.info("Fecha de inicio configurada")
            except:
                logger.warning("No se encontró el campo de fecha inicio con ID 'dateFrom'")
                # Intentar otros selectores comunes
                try:
                    fecha_inicio_field = self.driver.find_element(By.NAME, "dateFrom")
                    fecha_inicio_field.clear()
                    fecha_inicio_field.send_keys(fecha_inicio)
                    logger.info("Fecha de inicio configurada (name)")
                except:
                    logger.error("No se pudo encontrar el campo de fecha inicio")
                    return False
            
            # Buscar y configurar campo de fecha fin
            try:
                fecha_fin_field = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.ID, "dateTo"))
                )
                fecha_fin_field.clear()
                fecha_fin_field.send_keys(fecha_fin)
                logger.info("Fecha de fin configurada")
            except:
                logger.warning("No se encontró el campo de fecha fin con ID 'dateTo'")
                # Intentar otros selectores comunes
                try:
                    fecha_fin_field = self.driver.find_element(By.NAME, "dateTo")
                    fecha_fin_field.clear()
                    fecha_fin_field.send_keys(fecha_fin)
                    logger.info("Fecha de fin configurada (name)")
                except:
                    logger.error("No se pudo encontrar el campo de fecha fin")
                    return False
            
            # Buscar y hacer clic en el botón "Aplicar" o "Submit"
            try:
                # Intentar varios selectores para el botón de aplicar
                apply_button = None
                selectors = [
                    "button[type='submit']",
                    "input[type='submit']",
                    "//button[contains(text(), 'Aplicar')]",
                    "//button[contains(text(), 'Apply')]",
                    "//button[contains(text(), 'Filtrar')]",
                    ".btn-apply",
                    "#apply-button"
                ]
                
                for selector in selectors:
                    try:
                        if selector.startswith("//"):
                            apply_button = self.driver.find_element(By.XPATH, selector)
                        else:
                            apply_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        if apply_button and apply_button.is_displayed():
                            apply_button.click()
                            logger.info(f"Botón aplicar encontrado y clickeado: {selector}")
                            break
                    except:
                        continue
                
                if not apply_button:
                    logger.warning("No se encontró botón de aplicar, continuando sin aplicar fechas")
                    return True  # Continuar aunque no se apliquen las fechas
                
            except Exception as e:
                logger.warning(f"Error aplicando fechas: {str(e)}")
                return True  # Continuar aunque no se apliquen las fechas
            
            # Esperar a que se carguen los datos con las nuevas fechas
            logger.info("Esperando a que se carguen los datos con las fechas personalizadas...")
            time.sleep(5)
            
            return True
            
        except Exception as e:
            logger.error(f"Error configurando fechas personalizadas: {str(e)}")
            return False
    
    def extract_future_sessions_data(self):
        """Extrae específicamente los datos de sesiones futuras"""
        try:
            logger.info("Extrayendo datos de sesiones futuras...")
            
            # Buscar el contenedor de sesiones futuras
            try:
                future_sessions_container = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "top-sessions-sales"))
                )
                logger.info("Contenedor de sesiones futuras encontrado")
            except:
                logger.error("No se encontró el contenedor de sesiones futuras")
                return []
            
            extracted_data = []
            
            # Buscar todas las sesiones después del marcador "Sesiones futuras"
            try:
                # Buscar el elemento que marca las sesiones futuras
                future_marker = self.driver.find_element(By.ID, "future-sessions")
                logger.info("Marcador de sesiones futuras encontrado")
                
                # Buscar todas las sesiones que están después del marcador
                all_sessions = self.driver.find_elements(By.CSS_SELECTOR, "#wrapper-tops-sessions .body-sumary-sales")
                
                # Encontrar el índice del marcador de sesiones futuras
                future_marker_found = False
                session_count = 0
                
                for session in all_sessions:
                    try:
                        # Si encontramos el marcador de sesiones futuras, las próximas son futuras
                        if "future-sessions" in session.get_attribute("id") or "Sesiones futuras" in session.text:
                            future_marker_found = True
                            logger.info("Encontrado marcador de sesiones futuras")
                            continue
                        
                        # Solo procesar sesiones que están después del marcador
                        if future_marker_found and session.get_attribute("data-id-session"):
                            session_count += 1
                            session_data = self.extract_session_data(session, session_count)
                            if session_data:
                                extracted_data.append(session_data)
                                logger.info(f"Sesión futura extraída: {session_data.get('nombre', 'N/A')}")
                    
                    except Exception as e:
                        logger.warning(f"Error procesando sesión: {str(e)}")
                        continue
                
            except Exception as e:
                logger.warning(f"Error buscando sesiones futuras: {str(e)}")
                # Si no se encuentra el marcador, intentar extraer todas las sesiones con data-id-session
                try:
                    all_sessions = self.driver.find_elements(By.CSS_SELECTOR, "[data-id-session]")
                    for i, session in enumerate(all_sessions):
                        session_data = self.extract_session_data(session, i + 1)
                        if session_data:
                            extracted_data.append(session_data)
                except Exception as e2:
                    logger.error(f"Error extrayendo sesiones alternativo: {str(e2)}")
            
            logger.info(f"Total sesiones futuras extraídas: {len(extracted_data)}")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error extrayendo sesiones futuras: {str(e)}")
            return []
    
    def extract_session_data(self, session_element, index):
        """Extrae datos de una sesión específica"""
        try:
            session_data = {
                "fuente": "Coolco Ticket - Sesiones Futuras",
                "sesion_numero": index,
                "fecha_extraccion": datetime.now().isoformat(),
                "data_id_session": session_element.get_attribute("data-id-session")
            }
            
            # Extraer fecha
            try:
                month_elem = session_element.find_element(By.CSS_SELECTOR, ".calendar-icon-month")
                day_elem = session_element.find_element(By.CSS_SELECTOR, ".calendar-icon-day")
                session_data["mes"] = month_elem.text.strip()
                session_data["dia"] = day_elem.text.strip()
            except:
                session_data["mes"] = ""
                session_data["dia"] = ""
            
            # Extraer nombre del evento
            try:
                name_elem = session_element.find_element(By.CSS_SELECTOR, ".name-session")
                session_data["nombre"] = name_elem.text.strip()
            except:
                session_data["nombre"] = ""
            
            # Extraer valor total (precio)
            try:
                money_elem = session_element.find_element(By.CSS_SELECTOR, ".p4money")
                session_data["recaudacion"] = money_elem.text.strip()
                session_data["amount"] = money_elem.get_attribute("data-amount")
            except:
                session_data["recaudacion"] = ""
                session_data["amount"] = ""
            
            # Extraer cantidad de entradas
            try:
                tickets_elem = session_element.find_element(By.CSS_SELECTOR, ".value-total span:not(.p4money)")
                tickets_text = tickets_elem.text.strip()
                # Limpiar el texto para obtener solo el número
                session_data["entradas_vendidas"] = tickets_text.replace("", "").strip()
            except:
                session_data["entradas_vendidas"] = ""
            
            # Extraer URL de imagen
            try:
                img_elem = session_element.find_element(By.CSS_SELECTOR, ".banner img")
                session_data["imagen_url"] = img_elem.get_attribute("src")
            except:
                session_data["imagen_url"] = ""
            
            return session_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de sesión: {str(e)}")
            return None
    
    def extract_page_data(self):
        """Extrae datos de la página actual"""
        try:
            logger.info("Extrayendo datos de la página...")
            
            # Buscar todas las tablas
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            
            if not tables:
                logger.warning("No se encontraron tablas en la página")
                return []
            
            logger.info(f"Encontradas {len(tables)} tablas")
            
            extracted_data = []
            
            for table_index, table in enumerate(tables):
                try:
                    logger.info(f"Procesando tabla {table_index + 1}...")
                    
                    # Extraer headers
                    headers = []
                    try:
                        thead = table.find_element(By.TAG_NAME, "thead")
                        header_row = thead.find_element(By.TAG_NAME, "tr")
                        header_cells = header_row.find_elements(By.TAG_NAME, "th")
                        headers = [cell.text.strip() for cell in header_cells]
                        logger.info(f"Headers encontrados: {headers}")
                    except:
                        # Si no hay thead, intentar con la primera fila
                        try:
                            first_row = table.find_element(By.CSS_SELECTOR, "tr:first-child")
                            header_cells = first_row.find_elements(By.CSS_SELECTOR, "th, td")
                            headers = [cell.text.strip() for cell in header_cells]
                            logger.info(f"Headers extraídos de primera fila: {headers}")
                        except:
                            headers = [f"Columna_{i+1}" for i in range(10)]  # Headers genéricos
                    
                    # Extraer filas de datos
                    try:
                        tbody = table.find_element(By.TAG_NAME, "tbody")
                        data_rows = tbody.find_elements(By.TAG_NAME, "tr")
                    except:
                        # Si no hay tbody, buscar todas las filas menos la primera
                        all_rows = table.find_elements(By.TAG_NAME, "tr")
                        data_rows = all_rows[1:] if len(all_rows) > 1 else all_rows
                    
                    logger.info(f"Encontradas {len(data_rows)} filas de datos en tabla {table_index + 1}")
                    
                    for row_index, row in enumerate(data_rows):
                        try:
                            cells = row.find_elements(By.CSS_SELECTOR, "td, th")
                            
                            if cells and len(cells) > 0:
                                row_data = {
                                    "fuente": "Coolco Ticket",
                                    "tabla_numero": table_index + 1,
                                    "fila_numero": row_index + 1,
                                    "fecha_extraccion": datetime.now().isoformat(),
                                    "total_columnas": len(cells)
                                }
                                
                                # Mapear cada celda con su header correspondiente
                                for i, cell in enumerate(cells):
                                    header_name = headers[i] if i < len(headers) else f"Columna_{i+1}"
                                    cell_text = cell.text.strip()
                                    
                                    # Normalizar el nombre del header
                                    header_key = header_name.lower().replace(" ", "_").replace("ñ", "n")
                                    header_key = re.sub(r'[^\w]', '_', header_key)
                                    header_key = re.sub(r'_+', '_', header_key).strip('_')
                                    
                                    row_data[header_key] = cell_text if cell_text else ""
                                
                                extracted_data.append(row_data)
                                
                                # Log de las primeras filas
                                if row_index < 3:
                                    logger.info(f"  Tabla {table_index + 1}, Fila {row_index + 1}: {len(cells)} columnas extraídas")
                        
                        except Exception as e:
                            logger.warning(f"Error procesando fila {row_index + 1} de tabla {table_index + 1}: {str(e)}")
                            continue
                    
                    logger.info(f"✅ Tabla {table_index + 1} procesada exitosamente: {len(data_rows)} filas extraídas")
                    
                except Exception as e:
                    logger.error(f"Error procesando tabla {table_index + 1}: {str(e)}")
                    continue
            
            logger.info(f"Total de registros extraídos: {len(extracted_data)}")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de la página: {str(e)}")
            return []
    
    def save_data_to_json(self, data, filename_prefix="coolcoticket_data"):
        """Guarda los datos extraídos en un archivo JSON"""
        try:
            if not data:
                logger.warning("No hay datos para guardar")
                return None
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_filename = f"{filename_prefix}_{timestamp}.json"
            json_filepath = os.path.join(self.download_folder, json_filename)
            
            # Preparar datos para JSON
            json_data = {
                "fuente": "Coolco Ticket",
                "url": self.driver.current_url,
                "fecha_extraccion": datetime.now().isoformat(),
                "total_registros": len(data),
                "datos": data
            }
            
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Datos guardados en: {json_filepath}")
            return json_filepath
            
        except Exception as e:
            logger.error(f"Error guardando datos en JSON: {str(e)}")
            return None
    
    def analyze_page_content(self):
        """Analiza el contenido de la página actual"""
        try:
            logger.info("Analizando contenido de la página...")
            
            # Información básica de la página
            logger.info(f"Título de la página: {self.driver.title}")
            logger.info(f"URL actual: {self.driver.current_url}")
            
            # Contar elementos
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            forms = self.driver.find_elements(By.TAG_NAME, "form")
            buttons = self.driver.find_elements(By.TAG_NAME, "button")
            links = self.driver.find_elements(By.TAG_NAME, "a")
            
            logger.info(f"Tablas encontradas: {len(tables)}")
            logger.info(f"Formularios encontrados: {len(forms)}")
            logger.info(f"Botones encontrados: {len(buttons)}")
            logger.info(f"Enlaces encontrados: {len(links)}")
            
            # Mostrar parte del texto de la página
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            logger.info(f"Texto de la página (primeros 500 chars): {page_text[:500]}")
            
        except Exception as e:
            logger.error(f"Error analizando contenido de la página: {str(e)}")
    
    def close_driver(self):
        """Cierra el driver del navegador"""
        try:
            if self.driver:
                self.driver.quit()
                logger.info("Driver cerrado")
        except Exception as e:
            logger.error(f"Error cerrando el driver: {str(e)}")
    
    def run(self, url=None):
        """Ejecuta el scraper completo"""
        try:
            logger.info("=== INICIANDO SCRAPER DE COOLCO TICKET ===")
            
            # Configurar driver
            logger.info("PASO 1: Configurando driver...")
            if not self.setup_driver():
                logger.error("No se pudo configurar el driver")
                return False
            
            # Realizar login
            logger.info("PASO 2: Realizando login...")
            if not self.login():
                logger.error("No se pudo realizar el login")
                return False
            
            # Hacer clic en el botón "Personalizado"
            logger.info("PASO 3: Haciendo clic en el botón 'Personalizado'...")
            if not self.click_personalizado_button():
                logger.warning("No se pudo hacer clic en el botón 'Personalizado', continuando...")
            
            # Configurar fechas personalizadas
            logger.info("PASO 4: Configurando fechas personalizadas...")
            if not self.configure_custom_dates():
                logger.warning("No se pudieron configurar las fechas personalizadas, continuando...")
            
            # Si se proporciona una URL específica, navegar a ella después del login
            if url and url != self.login_url:
                logger.info(f"PASO 5: Navegando a URL específica: {url}")
                if not self.navigate_to_website(url):
                    logger.error("No se pudo navegar a la URL específica")
                    return False
            
            # Analizar contenido de la página
            logger.info("PASO 6: Analizando contenido de la página...")
            self.analyze_page_content()
            
            # Extraer datos de sesiones futuras específicamente
            logger.info("PASO 7: Extrayendo datos de sesiones futuras...")
            extracted_data = self.extract_future_sessions_data()
            
            # Guardar datos
            if extracted_data:
                logger.info("PASO 8: Guardando datos...")
                json_file = self.save_data_to_json(extracted_data, "coolcoticket_sesiones_futuras")
                
                if json_file:
                    logger.info("=== SCRAPER EJECUTADO EXITOSAMENTE ===")
                    logger.info(f"Total de registros extraídos: {len(extracted_data)}")
                    logger.info(f"Archivo JSON creado: {json_file}")
                    return True
                else:
                    logger.error("Error guardando los datos")
                    return False
            else:
                logger.warning("No se extrajeron datos")
                return False
                
        except Exception as e:
            logger.error(f"Error general en el scraper: {str(e)}")
            return False
        finally:
            # Cerrar navegador automáticamente
            logger.info("Cerrando navegador automáticamente...")
            self.close_driver()

def main():
    """Función principal"""
    logger.info("=== COOLCO TICKET SCRAPER AUTOMÁTICO ===")
    logger.info("Usuario: CamilaHalfon")
    logger.info("Contraseña: ****")
    logger.info("URL de login: https://ticketing.coolco.io/backoffice/login")
    logger.info("📅 Fechas: 01/01/2025 hasta 10 días después de hoy")
    logger.info("🎭 Extrayendo solo sesiones futuras")
    
    # Crear y ejecutar el scraper automáticamente
    scraper = CoolcoTicketScraper(headless=False)
    success = scraper.run(None)
    
    if success:
        print("✅ Scraper ejecutado exitosamente")
    else:
        print("❌ Error en la ejecución del scraper")

if __name__ == "__main__":
    main()
