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
import string
import platform
import subprocess
import pandas as pd
import pytz
from dateutil import parser
import psycopg2
from psycopg2.extras import RealDictCursor
from database_config import get_database_connection
import requests

# Configurar logging para Airflow (solo stream handler)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class NorteTicketScraper:
    def __init__(self, headless=True, test_mode=False):
        """
        Inicializa el scraper de NorteTicket para Airflow
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless (por defecto True para Airflow)
            test_mode (bool): Si True, solo extrae datos sin guardar en BD
        """
        self.driver = None
        self.headless = headless
        self.test_mode = test_mode
        self.base_url = "https://norteticket.com/auth/login/"
        
        # Configuración para Airflow (sin archivos físicos)
        self.download_folder = "/tmp"
        
        # Configuración de evasión de bots
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ]
        
        self.evasion_config = {
            "random_delays": True,
            "human_typing": True,
            "mouse_movement": True,
            "scroll_behavior": True
        }
        
        # Estructura de datos para almacenar información extraída
        self.final_data = {
            "events_data": [],
            "extraction_time": datetime.now().isoformat(),
            "username_used": "airbag",
            "url": self.base_url
        }
        
        # Conexión a base de datos (también en test para permitir matching sin escribir)
        self.db_connection = None
        self.setup_database_connection()
        
    def setup_database_connection(self):
        """Establece conexión con la base de datos PostgreSQL"""
        try:
            self.db_connection = get_database_connection()
            if self.db_connection:
                logger.info("✅ Conexión a base de datos establecida exitosamente")
            else:
                logger.error("❌ No se pudo establecer conexión a la base de datos")
        except Exception as e:
            logger.error(f"❌ Error estableciendo conexión a base de datos: {e}")
            self.db_connection = None
    
    def setup_driver(self):
        """Configura el driver de Chrome con evasión de bots"""
        try:
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument("--headless")
            
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Evasión de bots
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # User agent aleatorio
            user_agent = random.choice(self.user_agents)
            chrome_options.add_argument(f"--user-agent={user_agent}")
            
            try:
                # Intentar con webdriver-manager primero
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as e:
                logger.warning(f"Error con webdriver-manager: {e}")
                # Intentar sin service como fallback
                self.driver = webdriver.Chrome(options=chrome_options)
            
            # Ocultar webdriver
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("✅ Driver de Chrome configurado exitosamente con evasión de bots")
            return True
            
        except Exception as e:
            logger.error(f"Error al configurar el driver: {str(e)}")
            return False
    
    def navigate_to_login(self):
        """Navega a la página de login de NorteTicket"""
        try:
            logger.info(f"Navegando a: {self.base_url}")
            self.driver.get(self.base_url)
            
            # Esperar a que la página cargue completamente
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            logger.info("Página de login cargada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error al navegar a la página de login: {str(e)}")
            return False
    
    def get_page_info(self):
        """Obtiene información básica de la página"""
        try:
            title = self.driver.title
            current_url = self.driver.current_url
            
            logger.info(f"Título de la página: {title}")
            logger.info(f"URL actual: {current_url}")
            
            return {
                'title': title,
                'url': current_url
            }
            
        except Exception as e:
            logger.error(f"Error al obtener información de la página: {str(e)}")
            return None
    
    def debug_page_elements(self):
        """Debug: Muestra todos los elementos de input en la página"""
        try:
            logger.info("=== DEBUG: Analizando elementos de la página ===")
            
            # Buscar todos los inputs
            inputs = self.driver.find_elements(By.TAG_NAME, "input")
            logger.info(f"Total de inputs encontrados: {len(inputs)}")
            
            for i, input_elem in enumerate(inputs):
                try:
                    input_type = input_elem.get_attribute("type")
                    input_name = input_elem.get_attribute("name")
                    input_id = input_elem.get_attribute("id")
                    input_class = input_elem.get_attribute("class")
                    
                    logger.info(f"Input {i+1}: type='{input_type}', name='{input_name}', id='{input_id}', class='{input_class}'")
                except:
                    continue
            
            # Buscar todos los botones
            buttons = self.driver.find_elements(By.TAG_NAME, "button")
            logger.info(f"Total de botones encontrados: {len(buttons)}")
            
            for i, button in enumerate(buttons):
                try:
                    button_text = button.text
                    button_type = button.get_attribute("type")
                    button_class = button.get_attribute("class")
                    
                    logger.info(f"Botón {i+1}: text='{button_text}', type='{button_type}', class='{button_class}'")
                except:
                    continue
            
            logger.info("=== FIN DEBUG ===")
            
        except Exception as e:
            logger.error(f"Error en debug: {str(e)}")
    
    def wait_for_login_form(self):
        """Espera a que cargue el formulario de login"""
        try:
            logger.info("Esperando a que cargue el formulario de login...")
            
            # Esperar más tiempo para que la página cargue completamente
            time.sleep(5)
            
            # Usar los selectores exactos que encontramos en el debug
            username_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_username"))
            )
            logger.info("Campo de usuario encontrado con selector: (By.ID, 'id_username')")
            
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_password"))
            )
            logger.info("Campo de contraseña encontrado con selector: (By.ID, 'id_password')")
            
            logger.info("Formulario de login cargado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error esperando el formulario de login: {str(e)}")
            return False
    
    def random_delay(self, min_seconds=1, max_seconds=3):
        """Aplica un delay aleatorio para simular comportamiento humano"""
        if self.evasion_config.get("random_delays", True):
            delay = random.uniform(min_seconds, max_seconds)
            time.sleep(delay)
    
    def human_typing(self, element, text):
        """Simula escritura humana con delays aleatorios"""
        if self.evasion_config.get("human_typing", True):
            element.clear()
            for char in text:
                element.send_keys(char)
                time.sleep(random.uniform(0.05, 0.15))
        else:
            element.clear()
            element.send_keys(text)
    
    def random_mouse_movement(self):
        """Simula movimiento aleatorio del mouse"""
        if self.evasion_config.get("mouse_movement", True):
            try:
                # Mover mouse a posición aleatoria
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                self.driver.execute_script(f"window.scrollTo({x}, {y});")
                time.sleep(random.uniform(0.1, 0.3))
            except:
                pass
    
    def random_scroll(self):
        """Simula scroll aleatorio"""
        if self.evasion_config.get("scroll_behavior", True):
            try:
                scroll_amount = random.randint(100, 500)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(0.2, 0.5))
            except:
                pass
    
    def login(self, username, password):
        """Realiza el login con las credenciales proporcionadas"""
        try:
            logger.info("Iniciando proceso de login...")
            
            # Esperar a que el formulario esté listo
            if not self.wait_for_login_form():
                return False
            
            # Buscar y llenar el campo de username usando el selector correcto
            username_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_username"))
            )
            self.human_typing(username_field, username)
            logger.info(f"Usuario ingresado: {username}")
            self.random_delay(0.5, 1.5)
            
            # Buscar y llenar el campo de contraseña
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_password"))
            )
            self.human_typing(password_field, password)
            logger.info("Contraseña ingresada")
            self.random_delay(0.5, 1.5)
            
            # Buscar y hacer clic en el botón "ACCEDER" usando el selector correcto
            login_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@type='submit' and contains(text(), 'ACCEDER')]"))
            )
            login_button.click()
            logger.info("Botón ACCEDER clickeado")
            
            # Esperar un momento para que procese el login
            time.sleep(5)
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            logger.info(f"URL después del login: {current_url}")
            
            # Verificar si hay mensajes de error
            try:
                error_messages = self.driver.find_elements(By.CLASS_NAME, "alert-danger")
                if error_messages:
                    for error in error_messages:
                        logger.warning(f"Mensaje de error: {error.text}")
                    return False
            except:
                pass
            
            if "login" not in current_url.lower():
                logger.info("Login exitoso - Redirigido a otra página")
                return True
            else:
                logger.warning("Login posiblemente fallido - Aún en página de login")
                return False
                
        except Exception as e:
            logger.error(f"Error durante el login: {str(e)}")
            return False
    
    def close(self):
        """Cierra el driver del navegador y la conexión a la base de datos"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")
        if self.db_connection:
            self.db_connection.close()
            logger.info("🔌 Conexión a base de datos cerrada")
    
    def get_eventos_en_curso(self):
        """Obtiene la lista de eventos en curso desde la tabla (incluye ojito_url)."""
        try:
            logger.info("Obteniendo eventos en curso...")
            # Intentar esperar por id específico, si no aparece, fallback genérico por enlaces de ojito
            try:
                WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.ID, "eventos_en_curso"))
            )
                try:
                    WebDriverWait(self.driver, 5).until(
                        EC.invisibility_of_element_located((By.ID, "eventos_en_curso_processing"))
                    )
                except Exception:
                    pass
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#eventos_en_curso tbody tr"))
                )
                rows = self.driver.find_elements(By.CSS_SELECTOR, "#eventos_en_curso tbody tr")
            except Exception:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/administracion/evento/']"))
                )
                ojo_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/administracion/evento/']")
                rows = []
                for link in ojo_links:
                    try:
                        row = link.find_element(By.XPATH, "ancestor::tr")
                        rows.append(row)
                    except Exception:
                        continue
            logger.info(f"Encontrados {len(rows)} eventos en curso (determinados por enlaces de 'ojito')")
            
            eventos = []
            for i, row in enumerate(rows):
                try:
                    # Obtener información del evento
                    evento_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                    evento_nombre = evento_cell.find_element(By.TAG_NAME, "a").text.strip()
                    
                    # Limpiar el nombre del evento (remover información adicional como "Total Entradas Emitidas")
                    if "\n" in evento_nombre:
                        evento_nombre = evento_nombre.split("\n")[0].strip()
                    
                    # Obtener fecha del evento
                    fecha_cell = row.find_element(By.CSS_SELECTOR, "td:nth-child(2)")
                    fecha_evento = fecha_cell.text.strip()
                    
                    # Obtener recinto y ciudad
                    recinto_cell = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)")
                    recinto = recinto_cell.text.strip()
                    
                    ciudad_cell = row.find_element(By.CSS_SELECTOR, "td:nth-child(4)")
                    ciudad = ciudad_cell.text.strip()
                    
                    # Buscar enlaces de borderaux (silla) y ojito
                    borderaux_url = ""
                    ojito_url = ""
                    try:
                        borderaux_link = row.find_element(By.CSS_SELECTOR, "a.btn-circle.btn-outline-primary i.fa.fa-chair").find_element(By.XPATH, "./..")
                        borderaux_url = borderaux_link.get_attribute("href")
                    except Exception:
                        pass
                    try:
                        eye_link = row.find_element(By.CSS_SELECTOR, "td:last-child a[href*='/administracion/evento/']")
                        ojito_url = eye_link.get_attribute("href")
                    except Exception:
                        pass
                    
                    evento_info = {
                        'indice': i + 1,
                        'nombre': evento_nombre,
                        'fecha': fecha_evento,
                        'recinto': recinto,
                        'ciudad': ciudad,
                        'borderaux_url': borderaux_url,
                        'ojito_url': ojito_url
                    }
                    
                    eventos.append(evento_info)
                    logger.info(f"Evento {i+1}: {evento_nombre} - {fecha_evento} - {ciudad}")
                    
                except Exception as e:
                    logger.error(f"Error procesando fila {i+1}: {str(e)}")
                    continue
            
            return eventos
            
        except Exception as e:
            logger.error(f"Error obteniendo eventos en curso: {str(e)}")
            return []
    
    def click_borderaux(self, evento_info):
        """Hace clic en el botón borderaux (ícono de silla) de un evento específico"""
        try:
            logger.info(f"Haciendo clic en Borderaux para: {evento_info['nombre']}")
            
            # Buscar la fila del evento por su nombre
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#eventos_en_curso tbody tr")
            
            for row in rows:
                try:
                    evento_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                    evento_nombre = evento_cell.find_element(By.TAG_NAME, "a").text.strip()
                    
                    # Limpiar el nombre del evento (igual que en get_eventos_en_curso)
                    if "\n" in evento_nombre:
                        evento_nombre = evento_nombre.split("\n")[0].strip()
                    
                    logger.info(f"Comparando: '{evento_nombre}' con '{evento_info['nombre']}'")
                    
                    if evento_nombre == evento_info['nombre']:
                        # Encontrar y hacer clic en el botón borderaux (ícono de silla)
                        borderaux_link = row.find_element(By.CSS_SELECTOR, "a.btn-circle.btn-outline-primary i.fa.fa-chair").find_element(By.XPATH, "./..")
                        borderaux_link.click()
                        logger.info(f"✅ Clic exitoso en Borderaux para: {evento_info['nombre']}")
                        
                        # Esperar a que la página cargue
                        time.sleep(3)
                        return True
                        
                except Exception as e:
                    logger.error(f"Error procesando fila: {str(e)}")
                    continue
            
            logger.error(f"No se pudo encontrar el evento: {evento_info['nombre']}")
            return False
        except Exception as e:
            logger.error(f"Error haciendo clic en borderaux: {str(e)}")
            return False

    def click_ojito(self, evento_info):
        """Hace clic en el botón de ver (ícono de ojito) de un evento específico"""
        try:
            logger.info(f"Haciendo clic en Ojito para: {evento_info['nombre']}")
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#eventos_en_curso tbody tr")
            if not rows:
                rows = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            for row in rows:
                try:
                    evento_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                    evento_nombre = evento_cell.find_element(By.TAG_NAME, "a").text.strip()
                    if "\n" in evento_nombre:
                        evento_nombre = evento_nombre.split("\n")[0].strip()
                    if evento_nombre == evento_info['nombre']:
                        eye_link = None
                        try:
                            eye_link = row.find_element(By.CSS_SELECTOR, "td:last-child a.btn.btn-circle.btn-outline-primary i.fas.fa-eye").find_element(By.XPATH, "./..")
                        except Exception:
                            pass
                        if eye_link is None:
                            candidates = row.find_elements(By.CSS_SELECTOR, "td:last-child a[href*='/administracion/evento/']")
                            if candidates:
                                eye_link = candidates[0]
                        if eye_link is None:
                            logger.warning("No se encontró el enlace del ojito en la fila")
                            return False
                        href = eye_link.get_attribute("href")
                        eye_link.click()
                        logger.info(f"✅ Clic exitoso en Ojito para: {evento_info['nombre']} | URL: {href}")
                        time.sleep(3)
                        return True
                except Exception as e:
                    logger.error(f"Error procesando fila: {str(e)}")
                    continue
            logger.error(f"No se pudo encontrar el evento para ojito: {evento_info['nombre']}")
            return False
        except Exception as e:
            logger.error(f"Error haciendo clic en ojito: {str(e)}")
            return False

    def click_first_ojito_global(self):
        """Encuentra y clickea el primer enlace 'ojito' a /administracion/evento/{id} en la página actual."""
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/administracion/evento/']"))
            )
            links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/administracion/evento/']")
            if not links:
                logger.warning("No se encontraron enlaces de ojito globales")
                return False
            link = links[0]
            href = link.get_attribute("href")
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link)
            time.sleep(0.5)
            link.click()
            logger.info(f"✅ Clic global en ojito → {href}")
            time.sleep(2)
            return True
        except Exception as e:
            logger.warning(f"Fallo clic global en ojito: {e}")
            return False

    def parse_event_date_str(self, fecha_str):
        """Convierte 'DD/MM/YY HH:MM' u 'DD/MM/YY' a date (YYYY-MM-DD)."""
        try:
            fecha_part = fecha_str.strip()
            if ' ' in fecha_part:
                fecha_part = fecha_part.split(' ')[0]
            day, month, year = fecha_part.split('/')
            if len(year) == 2:
                year = f"20{year}"
            return datetime(int(year), int(month), int(day)).date()
        except Exception:
            return None

    def get_today_date_argentina(self):
        """Devuelve la fecha de hoy en Argentina (UTC-3), robusto cerca de medianoche."""
        try:
            tz = pytz.timezone('America/Argentina/Buenos_Aires')
            return datetime.now(tz).date()
        except Exception:
            return (datetime.utcnow() - timedelta(hours=3)).date()

    def extract_daily_metrics_from_dom(self):
        """Devuelve (monto_diario_ars, venta_diaria_tickets) desde Resumen Diario."""
        total_ventas_val = None
        ventas_diarias_val = None
        try:
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(normalize-space(.),'Resumen Diario')]"))
                )
            except Exception:
                pass
            def get_card_value_in_resumen_diario(header_text: str):
                try:
                    candidates = self.driver.find_elements(
                        By.XPATH,
                        "//*[contains(normalize-space(.),'Resumen Diario')]/following::div[contains(@class,'card')][.//div[contains(@class,'card-header')][contains(normalize-space(.), '" + header_text + "')]]"
                    )
                    if not candidates:
                        return ""
                    card = next((c for c in candidates if c.is_displayed()), candidates[0])
                    val_el = card.find_element(By.XPATH, ".//div[contains(@class,'card-title')]")
                    text = val_el.text.strip()
                    retries = 0
                    while (not text or text in ["-", "$"]) and retries < 8:
                        time.sleep(0.4)
                        text = val_el.text.strip()
                        retries += 1
                    return text
                except Exception:
                    return ""
            total_text = get_card_value_in_resumen_diario('Total Ventas')
            diarias_text = get_card_value_in_resumen_diario('Ventas Diarias')
            total_ventas_val = int(self.parse_currency(total_text)) if total_text else 0
            if re.search(r"[$]|ARS", diarias_text):
                ventas_diarias_val = None
            else:
                ventas_diarias_val = int(self.parse_number(diarias_text)) if diarias_text else 0
        except Exception:
            pass
        return total_ventas_val or 0, ventas_diarias_val if ventas_diarias_val is not None else 0

    def match_show_by_artist_and_date(self, nombre_evento, fecha_evento):
        """Busca show_id en BD por artista (antes de ' EN ') y fecha exacta (DATE(fecha_show))."""
        try:
            if not self.db_connection:
                self.setup_database_connection()
            if not self.db_connection:
                return None
            # Extraer artista: primero intentar por " EN ", luego buscar "AIRBAG" en el nombre
            if ' EN ' in nombre_evento:
                artista = nombre_evento.split(' EN ')[0].strip()
            elif 'AIRBAG' in nombre_evento.upper():
                artista = 'AIRBAG'
            else:
                artista = nombre_evento.strip()
            fecha_date = self.parse_event_date_str(fecha_evento)
            if not fecha_date:
                return None
            cursor = self.db_connection.cursor()
            query = (
                "select id from public.shows "
                "where ticketera='ticketnorteairbag' and upper(artista)=upper(%s) and date(fecha_show)=%s "
                "order by fecha_show desc limit 1"
            )
            cursor.execute(query, (artista, fecha_date))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return row[0]
            return None
        except Exception as e:
            logger.warning(f"Match por artista/fecha falló: {e}")
            return None
            
        except Exception as e:
            logger.error(f"Error haciendo clic en borderaux: {str(e)}")
            return False
    
    def extraer_info_borderaux(self, evento_info):
        """Extrae información de la tabla de borderaux/ventas de la página actual"""
        try:
            logger.info(f"Extrayendo información de borderaux para: {evento_info['nombre']}")
            
            # Esperar a que cargue la página del borderaux
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".table-responsive"))
            )
            
            # Extraer información del encabezado de la tabla
            try:
                nombre_evento_elem = self.driver.find_element(By.ID, "evento")
                nombre_evento = nombre_evento_elem.text.strip()
            except:
                nombre_evento = evento_info['nombre']
            
            try:
                fecha_emision_elem = self.driver.find_element(By.CSS_SELECTOR, ".headers")
                fecha_emision = fecha_emision_elem.text.strip()
            except:
                fecha_emision = ""
            
            # Estructura de datos para el borderaux
            info_borderaux = {
                'nombre_evento': nombre_evento,
                'fecha_evento': evento_info['fecha'],
                'recinto': evento_info['recinto'],
                'ciudad': evento_info['ciudad'],
                'fecha_emision': fecha_emision,
                'categorias': [],
                'resumen_total': {},
                'fecha_extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Extraer datos de la tabla
            try:
                # Buscar todas las filas de datos (excluyendo encabezados y total)
                rows = self.driver.find_elements(By.CSS_SELECTOR, ".table tbody tr:not(#titulos):not(#total)")
                logger.info(f"Encontradas {len(rows)} filas de categorías")
                
                for row in rows:
                    try:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) >= 8:  # Asegurar que tiene todas las columnas
                            
                            categoria = cells[0].text.strip()
                            cupos = cells[1].text.strip()
                            cupos_libres = cells[2].text.strip()
                            estado = cells[3].text.strip()
                            cantidad_vendida = cells[4].text.strip()
                            precio_punto_venta = cells[5].text.strip()
                            precio_online = cells[6].text.strip()
                            precio_descuento = cells[7].text.strip() if len(cells) > 7 else ""
                            total = cells[8].text.strip() if len(cells) > 8 else cells[7].text.strip()
                            
                            categoria_info = {
                                'categoria': categoria,
                                'cupos': self.limpiar_numero(cupos),
                                'cupos_libres': self.limpiar_numero(cupos_libres),
                                'estado': estado,
                                'cantidad_vendida': self.limpiar_numero(cantidad_vendida),
                                'precio_punto_venta': self.limpiar_precio(precio_punto_venta),
                                'precio_online': self.limpiar_precio(precio_online),
                                'precio_descuento': self.limpiar_precio(precio_descuento),
                                'total': self.limpiar_precio(total)
                            }
                            
                            info_borderaux['categorias'].append(categoria_info)
                            logger.info(f"✅ Categoría extraída: {categoria} - Vendida: {cantidad_vendida} - Total: {total}")
                            
                    except Exception as e:
                        logger.error(f"Error procesando fila de categoría: {str(e)}")
                        continue
                
                # Extraer fila de totales
                try:
                    total_row = self.driver.find_element(By.ID, "total")
                    total_cells = total_row.find_elements(By.TAG_NAME, "td")
                    
                    if len(total_cells) >= 8:
                        info_borderaux['resumen_total'] = {
                            'total_cupos': self.limpiar_numero(total_cells[1].text.strip()),
                            'total_cupos_libres': self.limpiar_numero(total_cells[2].text.strip()),
                            'total_cantidad_vendida': self.limpiar_numero(total_cells[4].text.strip()),
                            'total_general': self.limpiar_precio(total_cells[-1].text.strip())
                        }
                        logger.info(f"✅ Total general extraído: {info_borderaux['resumen_total']['total_general']}")
                        
                except Exception as e:
                    logger.error(f"Error extrayendo totales: {str(e)}")
                    
            except Exception as e:
                logger.error(f"Error extrayendo datos de la tabla: {str(e)}")
            
            # Agregar información a los datos finales
            self.final_data["events_data"].append(info_borderaux)
            logger.info(f"✅ Información de borderaux agregada a datos finales")
            logger.info(f"   📊 Evento: {info_borderaux['nombre_evento']}-{info_borderaux['fecha_evento']}-{info_borderaux['recinto']}")
            logger.info(f"   📊 Total vendido: {info_borderaux['resumen_total'].get('total_cantidad_vendida', 0)}")
            logger.info(f"   📊 Total recaudado: ${info_borderaux['resumen_total'].get('total_general', 0):,}")
            logger.info(f"✅ Información de borderaux extraída para: {evento_info['nombre']}")
            return True
            
        except Exception as e:
            logger.error(f"Error extrayendo información de borderaux: {str(e)}")
            return False
    
    def extraer_valor_monetario(self, texto):
        """Extrae valores monetarios del texto"""
        try:
            # Buscar patrones como "$ 74060000" o "$ 0"
            match = re.search(r'\$\s*([\d,]+)', texto)
            if match:
                return match.group(1).replace(',', '')
            return "0"
        except:
            return "0"
    
    def limpiar_numero(self, texto):
        """Limpia y extrae números del texto"""
        try:
            # Remover espacios y buscar números
            texto_limpio = texto.strip()
            match = re.search(r'(\d+)', texto_limpio)
            if match:
                return int(match.group(1))
            return 0
        except:
            return 0
    
    def limpiar_precio(self, texto):
        """Limpia y extrae valores de precio del texto"""
        try:
            # Remover espacios y buscar precios
            texto_limpio = texto.strip()
            if texto_limpio == "-" or texto_limpio == "":
                return 0
            
            # Buscar patrones como "ARS 55000" o "$ 270325000"
            match = re.search(r'(?:ARS|[$])\s*([\d,]+)', texto_limpio)
            if match:
                return int(match.group(1).replace(',', ''))
            
            # Si solo contiene números
            match = re.search(r'(\d+)', texto_limpio)
            if match:
                return int(match.group(1))
            
            return 0
        except:
            return 0
    
    def extraer_numero(self, texto):
        """Extrae números del texto"""
        try:
            # Buscar números en el texto
            match = re.search(r'(\d+)', texto)
            if match:
                return match.group(1)
            return "0"
        except:
            return "0"
    
    def volver_a_eventos(self):
        """Vuelve a la página de eventos en curso"""
        try:
            logger.info("Volviendo a la página de eventos en curso...")
            
            # Intentar volver usando el botón de navegación o URL directa
            self.driver.get("https://norteticket.com/administracion/")
            
            # Esperar a que cargue la página
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "eventos_en_curso"))
            )
            
            logger.info("✅ Vuelto exitosamente a la página de eventos")
            return True
            
        except Exception as e:
            logger.error(f"Error volviendo a eventos: {str(e)}")
            return False
    
    def parse_number(self, text):
        """Convierte texto a número entero"""
        try:
            if not text or text.strip() == "":
                return 0
            # Remover caracteres no numéricos excepto signos
            cleaned = re.sub(r'[^\d\-+]', '', str(text))
            return int(cleaned) if cleaned else 0
        except:
            return 0
    
    def parse_currency(self, text):
        """Convierte texto de moneda a número decimal"""
        try:
            if not text or text.strip() == "":
                return 0.0
            # Remover símbolos de moneda y espacios
            cleaned = re.sub(r'[^\d\.,]', '', str(text))
            # Reemplazar coma por punto para decimales
            cleaned = cleaned.replace(',', '.')
            return float(cleaned) if cleaned else 0.0
        except:
            return 0.0
    
    def parse_percentage(self, text):
        """Convierte texto de porcentaje a número decimal"""
        try:
            if not text or text.strip() == "":
                return 0.0
            # Remover símbolo de porcentaje
            cleaned = re.sub(r'[^\d\.,]', '', str(text))
            # Reemplazar coma por punto para decimales
            cleaned = cleaned.replace(',', '.')
            return float(cleaned) if cleaned else 0.0
        except:
            return 0.0
    
    def show_test_results(self, data):
        """Muestra los resultados en modo test sin guardar en BD"""
        try:
            print("\n" + "="*80)
            print("MODO TEST - DATOS EXTRAÍDOS DE TICKETNORTE AIRBAG")
            print("="*80)
            print(f"Fecha de extracción: {data.get('extraction_time', 'N/A')}")
            print(f"Usuario usado: {data.get('username_used', 'N/A')}")
            print(f"URL: {data.get('url', 'N/A')}")
            print("")
            
            if "events_data" in data and data["events_data"]:
                print(f"📊 EVENTOS ENCONTRADOS: {len(data['events_data'])}")
                print("-" * 80)
                
                for i, event in enumerate(data["events_data"], 1):
                    print(f"EVENTO {i}: {event.get('nombre_evento', 'Sin nombre')}")
                    print(f"   📍 Recinto: {event.get('recinto', 'Sin recinto')}")
                    print(f"   📅 Fecha: {event.get('fecha_evento', 'Sin fecha')}")
                    print(f"   🔗 URL Borderaux: {event.get('borderaux_url', 'N/A')}")
                    
                    # Mostrar resumen total
                    resumen = event.get("resumen_total", {})
                    if resumen:
                        print(f"   📊 RESUMEN DE VENTAS:")
                        print(f"      🎫 Total vendido: {resumen.get('total_cantidad_vendida', 0)}")
                        print(f"      💰 Total recaudado: ${resumen.get('total_general', 0):,}")
                        print(f"      🏟️ Total cupos: {resumen.get('total_cupos', 0)}")
                        
                        # Calcular ocupación
                        vendido = resumen.get('total_cantidad_vendida', 0)
                        cupos = resumen.get('total_cupos', 0)
                        ocupacion = (vendido / cupos * 100) if cupos > 0 else 0
                        print(f"      📈 Ocupación: {ocupacion:.2f}%")
                        print(f"      🎫 Disponibles: {cupos - vendido}")
                    
                    # Mostrar categorías
                    categorias = event.get("categorias", [])
                    if categorias:
                        print(f"   📋 CATEGORÍAS ({len(categorias)}):")
                        for cat in categorias[:3]:  # Mostrar solo las primeras 3
                            print(f"      - {cat.get('categoria', 'N/A')}: {cat.get('cantidad_vendida', 0)} vendidos")
                        if len(categorias) > 3:
                            print(f"      ... y {len(categorias) - 3} categorías más")
                    
                    print("")
                
                # Buscar shows matching en la BD
                print("🔍 BUSCANDO MATCHES EN LA BASE DE DATOS...")
                self.check_database_matches(data)
                
            else:
                print("⚠️ NO SE ENCONTRARON EVENTOS")
            
            print("="*80)
            print("MODO TEST COMPLETADO - Revisa los datos arriba")
            print("="*80)
            
        except Exception as e:
            logger.error(f"❌ Error mostrando resultados de test: {str(e)}")
    
    def try_api_extraction(self):
        """
        Intenta extraer datos usando la API después del login con Selenium
        Esto es más eficiente que navegar por todas las páginas
        """
        try:
            logger.info("🔍 PROBANDO EXTRACCIÓN VÍA API...")
            
            # Crear sesión requests con cookies de Selenium
            session = requests.Session()
            
            # Copiar cookies de Selenium a requests
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            
            # Headers para simular browser
            session.headers.update({
                'User-Agent': self.driver.execute_script("return navigator.userAgent;"),
                'Referer': 'https://norteticket.com/administracion/',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            })
            
            # Probar diferentes endpoints de API
            api_endpoints = [
                'https://norteticket.com/api/evento/',
                'https://norteticket.com/api/eventos/',
                'https://norteticket.com/administracion/api/eventos/',
                'https://norteticket.com/administracion/api/ventas/',
                'https://norteticket.com/administracion/api/borderaux/',
            ]
            
            api_data = {}
            
            for endpoint in api_endpoints:
                try:
                    logger.info(f"🔍 Probando endpoint: {endpoint}")
                    response = session.get(endpoint, timeout=10)
                    logger.info(f"   Status: {response.status_code}")
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            api_data[endpoint] = data
                            logger.info(f"   ✅ Datos JSON obtenidos: {type(data)}")
                            
                            if isinstance(data, list):
                                logger.info(f"   📋 Items encontrados: {len(data)}")
                                # Buscar eventos de AIRBAG
                                airbag_events = [item for item in data if 'airbag' in str(item).lower()]
                                if airbag_events:
                                    logger.info(f"   🎯 Eventos AIRBAG encontrados: {len(airbag_events)}")
                                    print(f"   🎯 EVENTOS AIRBAG VÍA API: {len(airbag_events)}")
                                    
                            elif isinstance(data, dict):
                                logger.info(f"   🔑 Keys disponibles: {list(data.keys())[:5]}")
                                
                        except json.JSONDecodeError:
                            logger.info(f"   📄 Respuesta no es JSON: {response.text[:100]}...")
                    else:
                        logger.info(f"   ❌ Error {response.status_code}")
                        
                except requests.RequestException as e:
                    logger.info(f"   ❌ Error de request: {e}")
                    
            return api_data
            
        except Exception as e:
            logger.error(f"❌ Error en extracción vía API: {e}")
            return {}
    
    def try_event_pages_extraction(self):
        """
        Explora las páginas específicas de eventos (el 'ojito' 👁️) para obtener datos detallados
        incluyendo posibles ventas diarias
        """
        try:
            logger.info("👁️ EXPLORANDO PÁGINAS ESPECÍFICAS DE EVENTOS...")
            
            # Crear sesión requests con cookies de Selenium
            session = requests.Session()
            
            # Copiar cookies de Selenium a requests
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            
            # Headers para simular browser
            session.headers.update({
                'User-Agent': self.driver.execute_script("return navigator.userAgent;"),
                'Referer': 'https://norteticket.com/administracion/',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            })
            
            # IDs de eventos conocidos para probar (basado en el ejemplo del usuario)
            event_ids = ['994']  # Podemos expandir esto más tarde
            
            for event_id in event_ids:
                try:
                    event_url = f'https://norteticket.com/administracion/evento/{event_id}'
                    logger.info(f"👁️ Explorando evento: {event_url}")
                    
                    response = session.get(event_url, timeout=15)
                    logger.info(f"   Status: {response.status_code}")
                    
                    if response.status_code == 200:
                        content = response.text
                        logger.info(f"   📄 Contenido obtenido: {len(content)} caracteres")
                        
                        # Analizar el contenido HTML para buscar datos de ventas
                        self.analyze_event_page_content(content, event_id)
                        
                    else:
                        logger.info(f"   ❌ Error {response.status_code}")
                        
                except requests.RequestException as e:
                    logger.info(f"   ❌ Error de request: {e}")
            
        except Exception as e:
            logger.error(f"❌ Error explorando páginas de eventos: {e}")
    
    def analyze_event_page_content(self, content, event_id):
        """
        Analiza el contenido HTML de una página de evento específico
        ENFOCADO SOLO EN VENTAS DIARIAS (sin sectores/borderaux)
        """
        try:
            logger.info(f"🔍 ANÁLISIS PROFUNDO DE VENTAS DIARIAS - Evento {event_id}...")
            print(f"\n🎯 ANÁLISIS DETALLADO DE VENTAS DIARIAS - EVENTO {event_id}")
            print("=" * 60)
            
            # 1. BUSCAR DATOS EN SCRIPTS JAVASCRIPT (GRÁFICOS)
            self.extract_daily_sales_from_scripts(content, event_id)
            
            # 2. BUSCAR VARIABLES CON DATOS DE VENTAS DIARIAS
            self.extract_daily_variables(content, event_id)
            
            # 3. BUSCAR ENDPOINTS AJAX/API PARA DATOS DIARIOS
            self.extract_ajax_endpoints(content, event_id)
            
            # 4. BUSCAR ELEMENTOS HTML CON DATOS DIARIOS
            self.extract_daily_html_elements(content, event_id)
            
        except Exception as e:
            logger.error(f"❌ Error en análisis profundo: {e}")
            self.basic_daily_analysis(content, event_id)
    
    def extract_daily_sales_from_scripts(self, content, event_id):
        """Extrae datos de ventas diarias de scripts JavaScript"""
        try:
            import re
            
            logger.info(f"📊 Buscando scripts con datos de ventas diarias...")
            
            # Buscar patrones de datos de gráficos/charts
            chart_patterns = [
                r'let\s+(\w*chart\w*)\s*=.*?(\[.*?\])',  # let myChart = [data]
                r'var\s+(\w*chart\w*)\s*=.*?(\[.*?\])',  # var chart = [data]
                r'data\s*:\s*(\[.*?\])',                  # data: [array]
                r'labels\s*:\s*(\[.*?\])',                # labels: [array]
                r'datasets\s*:\s*(\[.*?\])',              # datasets: [array]
                r'(\w*ventas\w*)\s*=\s*(\[.*?\])',       # ventas = [data]
                r'(\w*daily\w*)\s*=\s*(\[.*?\])',        # daily = [data]
                r'(\w*diaria\w*)\s*=\s*(\[.*?\])',       # diaria = [data]
            ]
            
            found_data = []
            
            for pattern in chart_patterns:
                matches = re.finditer(pattern, content, re.IGNORECASE | re.DOTALL)
                for match in matches:
                    if len(match.groups()) == 2:
                        var_name, data_array = match.groups()
                        logger.info(f"   ✅ Variable encontrada: {var_name}")
                        logger.info(f"   📊 Datos: {data_array[:100]}...")
                        print(f"   🎯 VARIABLE: {var_name}")
                        print(f"   📊 DATOS: {data_array[:200]}...")
                        found_data.append((var_name, data_array))
                    else:
                        data_array = match.group(1)
                        logger.info(f"   📊 Array de datos: {data_array[:100]}...")
                        print(f"   📊 ARRAY: {data_array[:200]}...")
                        found_data.append(("unknown", data_array))
            
            # Buscar configuraciones específicas de Chart.js o similares
            chart_config_patterns = [
                r'new\s+Chart\s*\([^)]+\)\s*\.\s*(\{.*?\})',
                r'Chart\s*\(\s*[^,]+,\s*(\{.*?\})\s*\)',
                r'chartjs.*?(\{.*?\})',
            ]
            
            for pattern in chart_config_patterns:
                matches = re.finditer(pattern, content, re.IGNORECASE | re.DOTALL)
                for match in matches:
                    config = match.group(1)[:300]
                    logger.info(f"   📈 Configuración de chart: {config}...")
                    print(f"   📈 CHART CONFIG: {config}...")
            
            if found_data:
                print(f"   ✅ ENCONTRADOS {len(found_data)} conjuntos de datos potenciales")
            else:
                print(f"   ❌ No se encontraron datos en scripts")
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo scripts: {e}")
    
    def extract_daily_variables(self, content, event_id):
        """Busca variables específicas con datos de ventas diarias"""
        try:
            import re
            
            logger.info(f"🔍 Buscando variables de ventas diarias...")
            
            # Patrones para variables de ventas diarias
            daily_patterns = [
                r'(activar_estadisticas)\s*=\s*[\'"]([^\'"]+)[\'"]',
                r'(mostrar_entradas_vendidas)\s*=\s*[\'"]([^\'"]+)[\'"]',
                r'(texto_ventas)\s*=\s*[\'"]([^\'"]*)[\'"]',
                r'(moneda)\s*=\s*[\'"]([^\'"]*)[\'"]',
                r'let\s+(\w*ventas\w*)\s*=\s*([^;]+)',
                r'var\s+(\w*ventas\w*)\s*=\s*([^;]+)',
                r'let\s+(\w*daily\w*)\s*=\s*([^;]+)',
                r'var\s+(\w*daily\w*)\s*=\s*([^;]+)',
            ]
            
            variables_found = []
            
            for pattern in daily_patterns:
                matches = re.finditer(pattern, content, re.IGNORECASE)
                for match in matches:
                    var_name, var_value = match.groups()
                    logger.info(f"   ✅ {var_name} = {var_value}")
                    print(f"   🎯 {var_name} = {var_value}")
                    variables_found.append((var_name, var_value))
            
            if variables_found:
                print(f"   ✅ ENCONTRADAS {len(variables_found)} variables de configuración")
            else:
                print(f"   ❌ No se encontraron variables específicas")
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo variables: {e}")
    
    def extract_ajax_endpoints(self, content, event_id):
        """Busca endpoints AJAX que puedan tener datos de ventas diarias"""
        try:
            import re
            
            logger.info(f"🌐 Buscando endpoints AJAX/API...")
            
            # Patrones para URLs de API
            api_patterns = [
                r'url\s*:\s*[\'"]([^\'"]*(?:api|ajax|data|ventas|sales|daily)[^\'"]*)[\'"]',
                r'fetch\s*\(\s*[\'"]([^\'"]+)[\'"]',
                r'axios\s*\.\s*get\s*\(\s*[\'"]([^\'"]+)[\'"]',
                r'jQuery\.get\s*\(\s*[\'"]([^\'"]+)[\'"]',
                r'\$\.get\s*\(\s*[\'"]([^\'"]+)[\'"]',
                r'XMLHttpRequest.*?open\s*\(\s*[\'"][^\'"]++[\'"],\s*[\'"]([^\'"]+)[\'"]',
            ]
            
            endpoints_found = []
            
            for pattern in api_patterns:
                matches = re.finditer(pattern, content, re.IGNORECASE)
                for match in matches:
                    endpoint = match.group(1)
                    if any(keyword in endpoint.lower() for keyword in ['api', 'data', 'ventas', 'sales', 'chart', 'estadisticas']):
                        logger.info(f"   🌐 Endpoint encontrado: {endpoint}")
                        print(f"   🌐 API ENDPOINT: {endpoint}")
                        endpoints_found.append(endpoint)
            
            if endpoints_found:
                print(f"   ✅ ENCONTRADOS {len(endpoints_found)} endpoints potenciales")
                # Intentar acceder a estos endpoints
                self.try_ajax_endpoints(endpoints_found, event_id)
            else:
                print(f"   ❌ No se encontraron endpoints AJAX")
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo endpoints: {e}")
    
    def try_ajax_endpoints(self, endpoints, event_id):
        """Intenta acceder a endpoints AJAX encontrados"""
        try:
            import requests
            
            # Crear sesión con cookies de Selenium
            session = requests.Session()
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            
            for endpoint in endpoints[:3]:  # Solo los primeros 3 para no sobrecargar
                try:
                    # Construir URL completa si es relativa
                    if endpoint.startswith('/'):
                        full_url = f"https://norteticket.com{endpoint}"
                    elif not endpoint.startswith('http'):
                        full_url = f"https://norteticket.com/administracion/{endpoint}"
                    else:
                        full_url = endpoint
                    
                    logger.info(f"   🔍 Probando endpoint: {full_url}")
                    response = session.get(full_url, timeout=10)
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            logger.info(f"   ✅ JSON obtenido: {type(data)}")
                            
                            # MOSTRAR DATOS COMPLETOS REALES
                            print(f"   ✅ DATOS JSON COMPLETOS:")
                            print(f"   " + "="*50)
                            
                            if isinstance(data, dict):
                                for key, value in data.items():
                                    if isinstance(value, list):
                                        print(f"   🔹 {key}: Lista con {len(value)} elementos")
                                        if len(value) <= 20:  # Si son pocos elementos, mostrarlos todos
                                            print(f"      Datos: {value}")
                                        else:  # Si son muchos, mostrar primeros y últimos
                                            print(f"      Primeros 5: {value[:5]}")
                                            print(f"      Últimos 5: {value[-5:]}")
                                    else:
                                        print(f"   🔹 {key}: {value}")
                            else:
                                print(f"   📊 Datos: {data}")
                            
                            print(f"   " + "="*50)
                        except:
                            logger.info(f"   📄 HTML/Text: {len(response.text)} chars")
                            if 'venta' in response.text.lower():
                                print(f"   ✅ CONTIENE DATOS DE VENTAS")
                    else:
                        logger.info(f"   ❌ Status: {response.status_code}")
                        
                except Exception as e:
                    logger.info(f"   ❌ Error: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Error probando endpoints: {e}")
    
    def extract_daily_html_elements(self, content, event_id):
        """Busca elementos HTML específicos con datos de ventas diarias"""
        try:
            from bs4 import BeautifulSoup
            
            logger.info(f"🔍 Buscando elementos HTML con ventas diarias...")
            soup = BeautifulSoup(content, 'html.parser')
            
            # Buscar elementos con IDs/clases relacionadas a ventas diarias
            daily_selectors = [
                '[id*="chart"]', '[class*="chart"]',
                '[id*="ventas"]', '[class*="ventas"]', 
                '[id*="sales"]', '[class*="sales"]',
                '[id*="daily"]', '[class*="daily"]',
                '[id*="diaria"]', '[class*="diaria"]',
                '[id*="estadistica"]', '[class*="estadistica"]',
            ]
            
            elements_found = []
            
            for selector in daily_selectors:
                elements = soup.select(selector)
                for element in elements:
                    element_info = {
                        'tag': element.name,
                        'id': element.get('id', 'N/A'),
                        'class': ' '.join(element.get('class', [])),
                        'text': element.get_text()[:100] + '...' if element.get_text() else 'N/A'
                    }
                    elements_found.append(element_info)
                    logger.info(f"   ✅ {element.name}#{element_info['id']}.{element_info['class']}")
                    print(f"   🎯 ELEMENTO: {element.name}#{element_info['id']}")
            
            if elements_found:
                print(f"   ✅ ENCONTRADOS {len(elements_found)} elementos HTML relacionados")
            else:
                print(f"   ❌ No se encontraron elementos HTML específicos")
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo elementos HTML: {e}")
    
    def basic_daily_analysis(self, content, event_id):
        """Análisis básico si fallan las herramientas avanzadas"""
        try:
            logger.info(f"🔍 Análisis básico de texto...")
            
            # Palabras clave para ventas diarias
            keywords = ['diaria', 'daily', 'chart', 'graph', 'estadistica', 'venta', 'data:', 'labels:']
            
            for keyword in keywords:
                if keyword.lower() in content.lower():
                    # Encontrar contexto alrededor de la palabra clave
                    import re
                    pattern = rf'.{{0,50}}{re.escape(keyword)}.{{0,50}}'
                    matches = re.finditer(pattern, content, re.IGNORECASE)
                    for i, match in enumerate(matches):
                        if i < 3:  # Solo primeros 3 matches
                            context = match.group(0)
                            print(f"   🎯 {keyword.upper()}: ...{context}...")
                            
        except Exception as e:
            logger.error(f"❌ Error en análisis básico: {e}")
    
    def update_daily_sales_from_endpoint(self):
        """Deprecated en AIRBAG: el flujo principal es via 'ojito' DOM + API. Conservado para compatibilidad."""
        logger.info("ℹ️ update_daily_sales_from_endpoint: método deprecado en AIRBAG; usar ingest_event_daily_sales")
        return False
    
    def find_show_id_automatically(self, artist_name="AIRBAG"):
        """
        Busca automáticamente el show_id de AIRBAG en la BD para futuros eventos
        """
        try:
            if not self.db_connection:
                logger.error("❌ No hay conexión a la base de datos para buscar show_id")
                return None
            
            cursor = self.db_connection.cursor()
            
            logger.info(f"🔍 Buscando shows de {artist_name} automáticamente...")
            
            # Buscar shows de AIRBAG con diferentes estrategias
            search_queries = [
                {
                    "query": """
                        SELECT id, artista, venue, fecha_show, capacidad_total 
                        FROM shows 
                        WHERE UPPER(artista) = UPPER(%s)
                        AND ticketera = 'ticketnorteairbag'
                        AND fecha_show >= CURRENT_DATE
                        ORDER BY fecha_show ASC
                        LIMIT 1
                    """,
                    "params": (artist_name,),
                    "description": f"Búsqueda futura: {artist_name} próximos shows"
                },
                {
                    "query": """
                        SELECT id, artista, venue, fecha_show, capacidad_total 
                        FROM shows 
                        WHERE UPPER(artista) = UPPER(%s)
                        AND ticketera = 'ticketnorteairbag'
                        ORDER BY fecha_show DESC
                        LIMIT 1
                    """,
                    "params": (artist_name,),
                    "description": f"Búsqueda general: {artist_name} último show"
                },
                {
                    "query": """
                        SELECT id, artista, venue, fecha_show, capacidad_total 
                        FROM shows 
                        WHERE UPPER(artista) LIKE UPPER(%s)
                        AND ticketera = 'ticketnorteairbag'
                        ORDER BY fecha_show DESC
                        LIMIT 1
                    """,
                    "params": (f"%{artist_name}%",),
                    "description": f"Búsqueda fuzzy: contiene '{artist_name}'"
                }
            ]
            
            # Ejecutar búsquedas en orden de prioridad
            for i, search in enumerate(search_queries, 1):
                logger.info(f"🔍 Estrategia {i}: {search['description']}")
                cursor.execute(search["query"], search["params"])
                results = cursor.fetchall()
                
                if results:
                    show = results[0]
                    show_id = show[0]
                    db_artista = show[1]
                    db_venue = show[2]
                    db_fecha = show[3]
                    db_capacidad = show[4]
                    
                    logger.info(f"✅ MATCH AUTOMÁTICO ENCONTRADO:")
                    logger.info(f"   Show ID: {show_id}")
                    logger.info(f"   Artista BD: {db_artista}")
                    logger.info(f"   Venue BD: {db_venue}")
                    logger.info(f"   Fecha: {db_fecha}")
                    logger.info(f"   Capacidad: {db_capacidad}")
                    
                    print(f"   ✅ Auto-match: {db_artista} en {db_venue} (ID: {show_id})")
                    return show_id
                else:
                    logger.info(f"   ❌ Sin resultados con estrategia {i}")
            
            # Fallback: usar el ID hardcodeado conocido
            logger.warning(f"⚠️ No se encontró show automático para {artist_name}, usando ID conocido")
            print(f"   ⚠️ Usando show conocido de {artist_name}")
            return "59175959-79a2-4ccd-a9f6-e122cf114663"  # Fallback al show conocido
            
        except Exception as e:
            logger.error(f"❌ Error buscando show_id automático: {e}")
            # Fallback al ID hardcodeado en caso de error
            return "59175959-79a2-4ccd-a9f6-e122cf114663"
    
    def extract_daily_sales_from_api(self, event_id):
        """
        Extrae datos del endpoint /api/cache/estadisticas/{event_id}/
        """
        try:
            import requests
            
            # Crear sesión con cookies de Selenium
            session = requests.Session()
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            
            # Headers para simular browser
            session.headers.update({
                'User-Agent': self.driver.execute_script("return navigator.userAgent;"),
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': f'https://norteticket.com/administracion/evento/{event_id}'
            })
            
            # Endpoint de estadísticas
            endpoint = f"https://norteticket.com/api/cache/estadisticas/{event_id}/"
            
            response = session.get(endpoint, timeout=15)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info(f"✅ Datos obtenidos del endpoint para evento {event_id}")
                    return data
                except Exception as e:
                    logger.error(f"❌ Error parseando JSON: {e}")
                    return None
            else:
                logger.error(f"❌ Error HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error accediendo al endpoint: {e}")
            return None
    
    def upsert_daily_sale_real(self, show_id, fecha_venta, venta_diaria, monto_diario_ars, venta_total_acumulada, recaudacion_total_ars, ticketera, url_origen=None, precio_promedio_ars=0):
        """UPSERT completo de daily_sales: siempre actualiza si existe, con todas las columnas."""
        try:
            if not self.db_connection:
                logger.error("❌ No hay conexión a la base de datos")
                return "error"
            
            cursor = self.db_connection.cursor()
            # Capacidad y métricas derivadas
            capacidad_total = 0
            try:
                cursor.execute("select capacidad_total from public.shows where id=%s", (show_id,))
                r = cursor.fetchone()
                if r and r[0]:
                    capacidad_total = int(r[0])
            except Exception:
                capacidad_total = 0
            tickets_disponibles = max(capacidad_total - (venta_total_acumulada or 0), 0) if capacidad_total else 0
            porcentaje_ocupacion = ((venta_total_acumulada or 0) / capacidad_total * 100.0) if capacidad_total else 0.0

            # Verificar existencia
            cursor.execute("""
                SELECT id FROM daily_sales WHERE show_id=%s AND fecha_venta=%s
            """, (show_id, fecha_venta))
            existing = cursor.fetchone()
            if existing:
                cursor.execute("""
                        UPDATE daily_sales SET
                        venta_diaria=%s,
                        monto_diario_ars=%s,
                        precio_promedio_ars=%s,
                        venta_total_acumulada=%s,
                        recaudacion_total_ars=%s,
                        tickets_disponibles=%s,
                        porcentaje_ocupacion=%s,
                        fecha_extraccion=%s,
                        ticketera=%s,
                        url_origen=%s,
                        updated_at=NOW()
                    WHERE show_id=%s AND fecha_venta=%s
                """, (
                    int(venta_diaria or 0),
                    int(monto_diario_ars or 0),
                    int(precio_promedio_ars or 0),
                    int(venta_total_acumulada or 0),
                    int(recaudacion_total_ars or 0),
                    int(tickets_disponibles or 0),
                    round(porcentaje_ocupacion, 2),
                    datetime.now(),
                    ticketera,
                    url_origen,
                    show_id,
                    fecha_venta
                ))
                self.db_connection.commit()
                return "updated"
            else:
                cursor.execute("""
                    INSERT INTO daily_sales (
                        show_id, fecha_venta, fecha_extraccion,
                        venta_diaria, monto_diario_ars, precio_promedio_ars,
                        venta_total_acumulada, recaudacion_total_ars,
                        tickets_disponibles, porcentaje_ocupacion,
                        ticketera, archivo_origen, url_origen
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    show_id,
                    fecha_venta,
                    datetime.now(),
                    int(venta_diaria or 0),
                    int(monto_diario_ars or 0),
                    int(precio_promedio_ars or 0),
                    int(venta_total_acumulada or 0),
                    int(recaudacion_total_ars or 0),
                    int(tickets_disponibles or 0),
                    round(porcentaje_ocupacion, 2),
                    ticketera,
                    'norteticket_scraper.py',
                    url_origen
                ))
                self.db_connection.commit()
                return "inserted"
                
        except Exception as e:
            logger.error(f"❌ Error en upsert_daily_sale_real: {e}")
            if self.db_connection:
                self.db_connection.rollback()
            return "error"

    def ingest_event_daily_sales(self, evento_info, test_only=True):
        """Abre ojito (o va directo por URL), lee métricas, calcula acumulados y hace upsert (o imprime)."""
        try:
            ojito_url = evento_info.get('ojito_url') if isinstance(evento_info, dict) else None
            if ojito_url:
                if ojito_url.startswith('/'):
                    ojito_url = f"https://norteticket.com{ojito_url}"
                self.driver.get(ojito_url)
                time.sleep(2)
            else:
                if not self.click_ojito(evento_info):
                    if not self.click_first_ojito_global():
                        print("❌ No se pudo abrir el ojito para ingesta")
                        return False
            monto_diario_ars, venta_diaria = self.extract_daily_metrics_from_dom()
            show_id = self.match_show_by_artist_and_date(evento_info['nombre'], evento_info['fecha'])
            if not show_id:
                print("⚠️ Sin match en BD; no se ingresa")
                self.volver_a_eventos()
                return False
            current_url = self.driver.current_url
            event_id = None
            m = re.search(r"/administracion/evento/(\d+)", current_url)
            if m:
                event_id = m.group(1)
            venta_total_acumulada = 0
            recaudacion_total_ars = 0
            precio_promedio = 0
            # Precio promedio del día si hay datos
            if venta_diaria > 0 and monto_diario_ars > 0:
                try:
                    precio_promedio = round(monto_diario_ars / max(venta_diaria, 1))
                except Exception:
                    precio_promedio = 0
            # Calcular acumulado desde la BD (no desde API)
            try:
                fecha_hoy = self.get_today_date_argentina()
                cur_acc = self.db_connection.cursor()
                # SIEMPRE tomar acumulado del último día previo y sumarle la venta del día actual
                # Esto asegura que el acumulado sea correcto incluso si ya existe un registro de hoy
                cur_acc.execute(
                    """
                    select venta_total_acumulada
                    from public.daily_sales
                    where show_id=%s and fecha_venta < %s
                    order by fecha_venta desc, updated_at desc
                    limit 1
                    """,
                    (show_id, fecha_hoy)
                )
                row_prev = cur_acc.fetchone()
                prev_acc = int(row_prev[0]) if row_prev and row_prev[0] is not None else 0
                venta_total_acumulada = prev_acc + int(venta_diaria or 0)
                cur_acc.close()
            except Exception:
                # Fallback: si falla, dejar acumulado en 0 (se corregirá en próxima corrida)
                venta_total_acumulada = int(venta_diaria or 0)
            # Calcular recaudación total acumulada (suma de montos diarios)
            try:
                fecha_hoy = self.get_today_date_argentina()
                cur_rec = self.db_connection.cursor()
                # Tomar recaudación del último día previo y sumarle el monto del día actual
                cur_rec.execute(
                    """
                    select recaudacion_total_ars
                    from public.daily_sales
                    where show_id=%s and fecha_venta < %s
                    order by fecha_venta desc, updated_at desc
                    limit 1
                    """,
                    (show_id, fecha_hoy)
                )
                row_prev_rec = cur_rec.fetchone()
                prev_recaudacion = int(row_prev_rec[0]) if row_prev_rec and row_prev_rec[0] is not None else 0
                recaudacion_total_ars = prev_recaudacion + int(monto_diario_ars or 0)
                cur_rec.close()
            except Exception:
                # Fallback: si falla, usar solo monto_diario_ars
                recaudacion_total_ars = int(monto_diario_ars or 0)
            
            # Precio promedio del día si hay datos
            if venta_diaria > 0 and monto_diario_ars > 0:
                try:
                    precio_promedio = round(monto_diario_ars / max(venta_diaria, 1))
                except Exception:
                    precio_promedio = 0
            # Si no hay precio del día, tomar último precio_promedio_ars no nulo
            if precio_promedio == 0:
                try:
                    cur2 = self.db_connection.cursor()
                    cur2.execute(
                        """
                        select precio_promedio_ars
                        from public.daily_sales
                        where show_id=%s and precio_promedio_ars is not null and precio_promedio_ars > 0
                        order by fecha_venta desc, updated_at desc
                        limit 1
                        """,
                        (show_id,)
                    )
                    r2 = cur2.fetchone()
                    cur2.close()
                    if r2 and r2[0]:
                        precio_promedio = int(r2[0])
                except Exception:
                    pass
            # Calcular disponibles y ocupación usando capacidad del show y acumulado calculado
            capacidad_total = 0
            tickets_disponibles = 0
            porcentaje_ocupacion = 0.0
            try:
                cur = self.db_connection.cursor()
                cur.execute("select capacidad_total from public.shows where id=%s", (show_id,))
                r = cur.fetchone()
                if r and r[0]:
                    capacidad_total = int(r[0])
                cur.close()
            except Exception:
                capacidad_total = 0
            if capacidad_total > 0:
                tickets_disponibles = max(capacidad_total - venta_total_acumulada, 0)
                porcentaje_ocupacion = (venta_total_acumulada / capacidad_total) * 100.0
            if test_only:
                fecha_arg = self.get_today_date_argentina()
                print(f"🧪 READY UPSERT → show_id={show_id} fecha={fecha_arg} tickets={venta_diaria} monto={monto_diario_ars} acumulado={venta_total_acumulada} recaudado={recaudacion_total_ars} precio_prom={precio_promedio} url={current_url}")
            else:
                result = self.upsert_daily_sale_real(
                    show_id=show_id,
                    fecha_venta=self.get_today_date_argentina(),
                    venta_diaria=venta_diaria,
                    monto_diario_ars=monto_diario_ars,
                    venta_total_acumulada=venta_total_acumulada,
                    recaudacion_total_ars=recaudacion_total_ars,
                    ticketera='ticketnorteairbag',
                    url_origen=current_url,
                    precio_promedio_ars=precio_promedio
                )
                print(f"💾 UPSERT daily_sales → {result}")
            self.volver_a_eventos()
            return True
        except Exception as e:
            print(f"❌ Ingesta fallida: {e}")
            try:
                self.volver_a_eventos()
            except:
                pass
            return False
    
    def check_database_matches(self, data):
        """Verifica matches con la base de datos en modo test"""
        try:
            # Conectar temporalmente para verificar matches
            temp_conn = get_database_connection()
            if not temp_conn:
                print("❌ No se pudo conectar a la BD para verificar matches")
                return
            
            cursor = temp_conn.cursor()
            
            # Buscar shows de AIRBAG en la BD
            cursor.execute("""
                SELECT id, artista, venue, fecha_show, capacidad_total
                FROM shows 
                WHERE UPPER(artista) LIKE '%AIRBAG%' 
                AND fecha_show >= CURRENT_DATE
                ORDER BY fecha_show
            """)
            
            db_shows = cursor.fetchall()
            print(f"📋 SHOWS DE AIRBAG EN LA BD: {len(db_shows)}")
            
            if db_shows:
                for show in db_shows:
                    show_id, artista, venue, fecha_show, capacidad = show
                    print(f"   - {artista} - {venue} - {fecha_show} (Cap: {capacidad})")
                
                print("\n🎯 MATCHING CON EVENTOS EXTRAÍDOS:")
                for event in data.get("events_data", []):
                    event_name = event.get('nombre_evento', '')
                    event_venue = event.get('recinto', '')
                    event_date = event.get('fecha_evento', '')
                    
                    print(f"   Evento: {event_name} - {event_venue} - {event_date}")
                    
                    # Buscar match simple por venue o fecha
                    match_found = False
                    for show in db_shows:
                        show_id, artista, venue, fecha_show, capacidad = show
                        if venue.lower() in event_venue.lower() or event_venue.lower() in venue.lower():
                            print(f"      ✅ POSIBLE MATCH: {artista} - {venue} (ID: {show_id})")
                            match_found = True
                            break
                    
                    if not match_found:
                        print(f"      ❌ NO MATCH encontrado")
            else:
                print("   ❌ No hay shows de AIRBAG en la BD")
            
            cursor.close()
            temp_conn.close()
            
        except Exception as e:
            logger.error(f"❌ Error verificando matches: {str(e)}")
    
    def save_data_to_database(self, data):
        """Guarda los datos extraídos en la base de datos"""
        try:
            if not self.db_connection:
                logger.error("❌ No hay conexión a la base de datos")
                return False

            # Ajustar zona horaria (restar 3 horas para Argentina)
            fecha_extraccion = datetime.now() - timedelta(hours=3)
            cursor = self.db_connection.cursor()
            raw_data_ids = []

            # Procesar cada evento individualmente
            if "events_data" in data and data["events_data"]:
                logger.info(f"🎯 Procesando {len(data['events_data'])} eventos individualmente...")

                for event in data["events_data"]:
                    try:
                        # Para TicketNorte AIRBAG, siempre usar "AIRBAG" como artista
                        artista = "AIRBAG"

                        # Crear JSON individual para cada evento
                        event_json = {
                            "artista": artista,
                            "venue": event.get("recinto", "TicketNorte Venue"),
                            "fecha_show": event.get("fecha_evento", ""),
                            "event_id": event.get("nombre_evento", "").replace(" ", "_").lower(),
                            "event_url": event.get("borderaux_url", ""),
                            "categorias": event.get("categorias", []),
                            "resumen_total": event.get("resumen_total", {}),
                            "extraction_time": data.get("extraction_time", ""),
                            "username_used": data.get("username_used", ""),
                            "url": data.get("url", "")
                        }

                        # Calcular totales numéricos
                        total_vendido = event.get("resumen_total", {}).get("total_cantidad_vendida", 0)
                        total_recaudado = event.get("resumen_total", {}).get("total_general", 0)
                        total_cupos = event.get("resumen_total", {}).get("total_cupos", 0)
                        
                        # Calcular porcentaje de ocupación
                        porcentaje_ocupacion = 0
                        if total_cupos > 0:
                            porcentaje_ocupacion = (total_vendido / total_cupos) * 100

                        # Agregar totales calculados al JSON
                        event_json["totales"] = {
                            "total_vendido": total_vendido,
                            "total_recaudado": total_recaudado,
                            "total_capacidad": total_cupos,
                            "porcentaje_ocupacion": porcentaje_ocupacion
                        }

                        # Insertar en raw_data con columnas separadas
                        insert_query = """
                            INSERT INTO raw_data (
                                ticketera,
                                json_data,
                                fecha_extraccion,
                                archivo_origen,
                                url_origen,
                                procesado,
                                artista,
                                venue,
                                fecha_show
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING id;
                        """

                        archivo_origen = f"ticketnorteairbag_{event.get('nombre_evento', 'evento').replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        url_origen = event.get("borderaux_url", self.base_url)
                        json_data_str = json.dumps(event_json, ensure_ascii=False)

                        # Convertir fecha_show a timestamp
                        fecha_show_timestamp = None
                        if event.get("fecha_evento"):
                            try:
                                # Parsear fecha en formato DD/MM/YY HH:MM (año de 2 dígitos con hora)
                                fecha_str = event.get("fecha_evento")
                                # Separar fecha y hora
                                if " " in fecha_str:
                                    fecha_part, hora_part = fecha_str.split(" ", 1)
                                else:
                                    fecha_part = fecha_str
                                
                                # Parsear solo la parte de la fecha
                                fecha_parts = fecha_part.split("/")
                                if len(fecha_parts) == 3:
                                    day, month, year = fecha_parts
                                    # Convertir año de 2 dígitos a 4 dígitos
                                    year_4_digits = int(year)
                                    if year_4_digits < 100:
                                        year_4_digits += 2000
                                    fecha_show_timestamp = datetime(year_4_digits, int(month), int(day))
                            except Exception as e:
                                logger.warning(f"Error parseando fecha {event.get('fecha_evento')}: {e}")
                                fecha_show_timestamp = None

                        cursor.execute(insert_query, (
                            'ticketnorteairbag',
                            json_data_str,
                            fecha_extraccion,
                            archivo_origen,
                            url_origen,
                            False,  # No procesado aún, el trigger lo procesará
                            artista,  # Usar la variable artista extraída
                            event_json['venue'],
                            fecha_show_timestamp
                        ))

                        raw_data_id = cursor.fetchone()[0]
                        raw_data_ids.append(raw_data_id)

                        logger.info(f"✅ Evento '{event.get('nombre_evento', 'Sin título')}' guardado con ID: {raw_data_id}")
                        logger.info(f"   📊 Tickets: {total_vendido}, Recaudado: ${total_recaudado:,}, Capacidad: {total_cupos}")

                    except Exception as e:
                        logger.error(f"❌ Error guardando evento '{event.get('nombre_evento', 'Sin título')}': {e}")
                        continue

                # Confirmar transacción
                self.db_connection.commit()
                logger.info(f"✅ {len(raw_data_ids)} eventos guardados exitosamente en base de datos")
                logger.info(f"📊 Usuario utilizado: {data.get('username_used', 'N/A')}")
                logger.info(f"🔄 El trigger automático procesará los datos")
                return True

            else:
                logger.warning("⚠️ No hay datos de eventos para guardar")
                return False

        except Exception as e:
            logger.error(f"❌ Error guardando datos en base de datos: {e}")
            if self.db_connection:
                self.db_connection.rollback()
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def run(self):
        """Ejecuta el proceso completo de scraping"""
        try:
            logger.info("=== INICIANDO SCRAPER DE TICKETNORTE AIRBAG PARA AIRFLOW ===")
            
            # Configurar driver
            if not self.setup_driver():
                return False
            
            # Navegar y hacer login
            if not self.navigate_to_login():
                return False
            
            if not self.login("airbag", "norteticket"):
                return False
            
            # Grilla de eventos y flujo por 'ojito' con ingesta DOM + API + upsert
            eventos = self.get_eventos_en_curso()
            if not eventos:
                logger.warning("No se encontraron eventos en curso")
                return False
            
            logger.info(f"📊 Encontrados {len(eventos)} eventos en curso")
            
            if self.test_mode:
                logger.info("🧪 MODO TEST: exploración de 'ojito' sin escribir en BD")
                for i, evento in enumerate(eventos, 1):
                    logger.info(f"➡️ Test {i}/{len(eventos)}: {evento['nombre']} | {evento['fecha']} | {evento['ciudad']}")
                    self.ingest_event_daily_sales(evento, test_only=True)
            else:
                logger.info("💾 MODO PRODUCCIÓN: ingesta daily_sales desde 'Resumen Diario'")
                for i, evento in enumerate(eventos, 1):
                    logger.info(f"➡️ Ingestando {i}/{len(eventos)}: {evento['nombre']} | {evento['fecha']} | {evento['ciudad']}")
                    self.ingest_event_daily_sales(evento, test_only=False)
            
            logger.info("🎉 Scraper completado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error durante el scraping: {e}")
            return False
        finally:
            self.close()

def main():
    """Función principal para ejecutar el scraper"""
    scraper = None
    
    try:
        # Inicializar scraper
        scraper = NorteTicketScraper(headless=False)  # False para ver el navegador
        
        # Configurar driver
        if not scraper.setup_driver():
            logger.error("No se pudo configurar el driver")
            return
        
        # Navegar a la página de login
        if not scraper.navigate_to_login():
            logger.error("No se pudo navegar a la página de login")
            return
        
        # Obtener información de la página
        page_info = scraper.get_page_info()
        if page_info:
            print(f"\nInformación de la página:")
            print(f"Título: {page_info['title']}")
            print(f"URL: {page_info['url']}")
        
        # Debug: Analizar elementos de la página
        print("\nAnalizando elementos de la página...")
        scraper.debug_page_elements()
        
        # Realizar login
        username = "airbag"
        password = "norteticket"
        
        print(f"\nIntentando login con usuario: {username}")
        
        if scraper.login(username, password):
            print("✅ Login exitoso!")
            
            # Obtener información después del login
            new_page_info = scraper.get_page_info()
            if new_page_info:
                print(f"\nDespués del login:")
                print(f"Título: {new_page_info['title']}")
                print(f"URL: {new_page_info['url']}")
            
            # Procesar eventos en curso
            print("\n=== PROCESANDO EVENTOS EN CURSO ===")
            eventos = scraper.get_eventos_en_curso()
            
            if eventos:
                print(f"\nEncontrados {len(eventos)} eventos en curso:")
                for evento in eventos:
                    print(f"- {evento['nombre']} ({evento['fecha']}) - {evento['ciudad']}")
                
                # Procesar cada evento
                for i, evento in enumerate(eventos):
                    print(f"\n--- Procesando evento {i+1}/{len(eventos)}: {evento['nombre']} ---")
                    
                    # Hacer clic en borderaux
                    if scraper.click_borderaux(evento):
                        print(f"✅ Abierto borderaux para: {evento['nombre']}")
                        
                        # Extraer información del borderaux
                        if scraper.extraer_info_borderaux(evento):
                            print(f"✅ Información de borderaux extraída para: {evento['nombre']}")
                        else:
                            print(f"❌ Error extrayendo información de borderaux para: {evento['nombre']}")
                        
                        # Volver a la página de eventos
                        if scraper.volver_a_eventos():
                            print("✅ Vuelto a la página de eventos")
                        else:
                            print("❌ Error volviendo a eventos")
                            break
                    else:
                        print(f"❌ Error abriendo borderaux para: {evento['nombre']}")
                        break
                
                print("\n✅ Procesamiento de eventos completado")
            else:
                print("❌ No se encontraron eventos en curso")
        else:
            print("❌ Login fallido")
        
        # Mantener la página abierta para verificar
        print("\nProceso completado. Presiona Enter para cerrar...")
        # input()  # Comentado para ejecución automática
        
    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
    finally:
        if scraper:
            scraper.close()

def main():
    """Función principal para ejecutar el scraper en modo producción"""
    try:
        scraper = NorteTicketScraper(headless=True, test_mode=False)
        return scraper.run()
    except Exception as e:
        logger.error(f"❌ Error en función main: {e}")
        return False

def main_test():
    """Función principal para ejecutar el scraper en modo test"""
    try:
        logger.info("🧪 INICIANDO TICKETNORTE AIRBAG EN MODO TEST")
        scraper = NorteTicketScraper(headless=True, test_mode=True)
        return scraper.run()
    except Exception as e:
        logger.error(f"❌ Error en main_test: {e}")
        return False

if __name__ == "__main__":
    # Ejecutar en modo producción para actualizar ventas diarias
    main()
