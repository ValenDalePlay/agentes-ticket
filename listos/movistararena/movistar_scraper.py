from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import logging
import os
from datetime import datetime, timedelta, timezone
import json
import re
import random
import string
import platform
import subprocess
import pandas as pd
from bs4 import BeautifulSoup
import pytz
from dateutil import parser
import psycopg2
from psycopg2.extras import RealDictCursor
from database_config import get_database_connection

# Configurar logging para Airflow (sin archivos físicos)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Solo logging a consola para Airflow
    ]
)
logger = logging.getLogger(__name__)

class MovistarArenaVentasFuncionScraper:
    def __init__(self, headless=False):  # Por defecto visible para desarrollo
        """
        Inicializa el scraper de Movistar Arena para Ventas por Función optimizado para Airflow
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless (recomendado para contenedores)
        """
        self.driver = None
        self.headless = headless
        self.base_url = "https://backoffice-movistararena.fanfactory.com.ar/productor/ventasporfuncion"
        
        # Configuración para contenedores (no crear carpetas físicas)
        self.download_folder = "/tmp"  # Usar /tmp en contenedores
        
        # Configuración de evasión de bots
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        ]
        
        # Configuración de evasión
        self.evasion_config = {
            "random_delays": True,
            "human_typing": True,
            "mouse_movement": True,
            "scroll_behavior": True,
            "window_resize": True,
            "fingerprint_spoofing": True
        }
        
        # Inicializar lista para tablas extraídas (en memoria)
        self.extracted_tables = []
        
        # Lista de artistas específicos a procesar (FILTRO)
        self.artistas_permitidos = [
            "Carlos Vives",
            "Cazzu", 
            "Diego Torres",
            "Duki",
            "Eladio Carrion",
            "ERREWAY",  # Variaciones posibles: ERREWAY, ERREWEY, Erreway
            "ERREWEY",  # Agregando también esta variación por si aparece así
            "Rauw Alejandro"
        ]
        
        # Datos finales para retornar (sin archivos físicos)
        self.final_data = {
            "ticketera": "movistar",
            "fecha_extraccion": None,
            "total_artistas_procesados": 0,
            "artistas_exitosos": 0,
            "artistas_con_error": 0,
            "datos_por_artista": {}
        }
        
        logger.info("=== INICIALIZACIÓN DEL SCRAPER MOVISTAR ARENA VENTAS POR FUNCIÓN PARA AIRFLOW ===")
        logger.info(f"URL objetivo: {self.base_url}")
        logger.info(f"Modo headless: {self.headless}")
        logger.info(f"Modo contenedor: Sin archivos físicos")
        logger.info("🎯 ARTISTAS ESPECÍFICOS A PROCESAR (FILTRO ACTIVO):")
        for i, artista in enumerate(self.artistas_permitidos, 1):
            if artista == "Diego Torres":
                logger.info(f"  🎯 {i}. {artista} (OPCIÓN 2)")
            else:
                logger.info(f"  🎯 {i}. {artista}")
        logger.info("🛡️ MODOS DE EVASIÓN ACTIVADOS:")
        for key, value in self.evasion_config.items():
            logger.info(f"  🛡️ {key}: {'✅' if value else '❌'}")
        
        # Inicializar conexión a base de datos
        self.db_connection = None
        self.setup_database_connection()
    
    def is_artist_allowed(self, artist_name):
        """
        Verifica si un artista está en la lista de artistas permitidos
        
        Args:
            artist_name (str): Nombre del artista a verificar
            
        Returns:
            bool: True si el artista está permitido, False en caso contrario
        """
        try:
            # Normalizar nombre del artista para comparación (sin espacios extra, mayúsculas/minúsculas)
            artist_normalized = artist_name.strip()
            
            # DEBUG específico para ERREWAY
            if "erreway" in artist_normalized.lower() or "errewey" in artist_normalized.lower():
                logger.info(f"🔍 DEBUG ERREWAY - Verificando: '{artist_normalized}'")
                logger.info(f"🔍 DEBUG ERREWAY - Lista permitidos: {self.artistas_permitidos}")
            
            # Verificar coincidencia exacta o parcial con los artistas permitidos
            for artista_permitido in self.artistas_permitidos:
                # Comparación exacta (case-insensitive)
                if artist_normalized.lower() == artista_permitido.lower():
                    if "erreway" in artist_normalized.lower():
                        logger.info(f"✅ DEBUG ERREWAY - Match exacto: '{artist_normalized}' == '{artista_permitido}'")
                    return True
                
                # Comparación parcial - el artista permitido está contenido en el nombre encontrado
                if artista_permitido.lower() in artist_normalized.lower():
                    if "erreway" in artist_normalized.lower():
                        logger.info(f"✅ DEBUG ERREWAY - Match parcial: '{artista_permitido}' en '{artist_normalized}'")
                    return True
                
                # Comparación parcial inversa - el nombre encontrado está contenido en el artista permitido
                if artist_normalized.lower() in artista_permitido.lower():
                    if "erreway" in artist_normalized.lower():
                        logger.info(f"✅ DEBUG ERREWAY - Match inverso: '{artist_normalized}' en '{artista_permitido}'")
                    return True
            
            # DEBUG específico para ERREWAY si no matchea
            if "erreway" in artist_normalized.lower() or "errewey" in artist_normalized.lower():
                logger.warning(f"❌ DEBUG ERREWAY - NO MATCH para: '{artist_normalized}'")
            
            return False
            
        except Exception as e:
            logger.error(f"Error verificando artista permitido '{artist_name}': {str(e)}")
            return False
    
    def setup_database_connection(self):
        """Verifica conexión con la base de datos PostgreSQL"""
        try:
            logger.info("🔌 Verificando conexión con la base de datos...")
            
            # Probar conexión
            connection = get_database_connection()
            if connection:
                cursor = connection.cursor()
                cursor.execute("SELECT NOW();")
                result = cursor.fetchone()
                logger.info(f"✅ Conexión exitosa! Hora actual: {result[0]}")
                cursor.close()
                connection.close()
                self.db_connected = True
            else:
                logger.warning("⚠️ No se pudo establecer conexión con la base de datos")
                self.db_connected = False
                
        except Exception as e:
            logger.error(f"❌ Error verificando conexión con la base de datos: {str(e)}")
            logger.warning("⚠️ El scraper funcionará pero no guardará datos en la BD")
            self.db_connected = False
    
    def setup_download_folder(self):
        """Configura la carpeta temporal para contenedores"""
        try:
            # En contenedores, usar /tmp que siempre existe
            if not os.path.exists(self.download_folder):
                os.makedirs(self.download_folder, exist_ok=True)
                logger.info(f"✅ Carpeta temporal configurada: {self.download_folder}")
            else:
                logger.info(f"📁 Carpeta temporal ya existe: {self.download_folder}")
        except Exception as e:
            logger.error(f"❌ Error configurando carpeta temporal: {str(e)}")
            # Fallback a /tmp
            self.download_folder = "/tmp"
            logger.info(f"🔄 Usando fallback: {self.download_folder}")
    
    def setup_driver(self):
        """Configura el driver de Chrome optimizado para contenedores"""
        try:
            logger.info("🔧 Configurando driver de Chrome para desarrollo (visible)...")
            
            chrome_options = Options()
            
            # Configuración para desarrollo - mostrar navegador si headless=False
            if self.headless:
                chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            # Comentar estas líneas para desarrollo para ver mejor la página
            # chrome_options.add_argument("--disable-images")  # Optimizar para contenedores
            # chrome_options.add_argument("--disable-javascript")  # Solo si no es necesario
            
            # Configurar user agent
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Configurar carpeta de descargas temporal
            chrome_options.add_experimental_option("prefs", {
                "download.default_directory": self.download_folder,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            })
            
            logger.info("⚙️ Opciones de Chrome configuradas para desarrollo")
            
            # Intentar configurar el driver de forma simple
            try:
                logger.info("🔄 Intentando configuración simple del driver...")
                self.driver = webdriver.Chrome(options=chrome_options)
                logger.info("✅ Driver configurado con configuración simple")
            except Exception as e:
                logger.warning(f"⚠️ Error con configuración simple: {str(e)}")
                logger.info("🔄 Intentando con webdriver-manager como respaldo...")
                # Intentar con webdriver-manager como respaldo
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info("✅ Driver configurado con webdriver-manager")
            
            logger.info("🎉 Driver de Chrome configurado exitosamente para desarrollo")
            
            # Aplicar evasión avanzada después de configurar el driver
            self.apply_advanced_evasion()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error al configurar el driver: {str(e)}")
            return False
    
    def navigate_to_page(self):
        """Navega a la página de Movistar Arena y cuenta elementos específicos"""
        try:
            logger.info("🌐 Navegando a la página de Movistar Arena...")
            logger.info(f"📍 URL: {self.base_url}")
            
            self.driver.get(self.base_url)
            
            # Esperar a que la página cargue completamente
            logger.info("⏳ Esperando a que la página cargue...")
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Obtener el título de la página
            page_title = self.driver.title
            logger.info(f"📄 Título de la página: {page_title}")
            
            # Obtener la URL actual
            current_url = self.driver.current_url
            logger.info(f"🔗 URL actual: {current_url}")
            
            # Verificar si estamos en la página correcta
            if "movistar" in current_url.lower() or "fanfactory" in current_url.lower():
                logger.info("✅ Página de Movistar Arena cargada exitosamente")
            else:
                logger.warning("⚠️ Posible redirección a otra página")
            
            # Contar divs con clase 'mud-select mud-autocomplete'
            divs = self.driver.find_elements(By.CSS_SELECTOR, "div.mud-select.mud-autocomplete")
            logger.info(f"🔢 Cantidad de divs con clase 'mud-select mud-autocomplete': {len(divs)}")
            print(f"DIVS ENCONTRADOS: {len(divs)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error navegando a la página: {str(e)}")
            return False
    
    def analyze_page_structure(self):
        """Analiza la estructura de la página para entender qué elementos están disponibles"""
        try:
            logger.info("🔍 Analizando estructura de la página...")
            
            # Obtener todos los elementos de texto visibles
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            logger.info(f"📝 Contenido del body (primeros 500 caracteres): {body_text[:500]}...")
            
            # Buscar formularios
            forms = self.driver.find_elements(By.TAG_NAME, "form")
            logger.info(f"📋 Formularios encontrados: {len(forms)}")
            
            for i, form in enumerate(forms):
                try:
                    form_action = form.get_attribute("action")
                    form_method = form.get_attribute("method")
                    logger.info(f"  📋 Formulario {i+1}: action={form_action}, method={form_method}")
                except:
                    pass
            
            # Buscar campos de input
            inputs = self.driver.find_elements(By.TAG_NAME, "input")
            logger.info(f"⌨️ Campos de input encontrados: {len(inputs)}")
            
            for i, input_elem in enumerate(inputs):
                try:
                    input_type = input_elem.get_attribute("type")
                    input_name = input_elem.get_attribute("name")
                    input_id = input_elem.get_attribute("id")
                    input_placeholder = input_elem.get_attribute("placeholder")
                    
                    logger.info(f"  ⌨️ Input {i+1}: type={input_type}, name={input_name}, id={input_id}, placeholder={input_placeholder}")
                except:
                    pass
            
            # Buscar botones
            buttons = self.driver.find_elements(By.TAG_NAME, "button")
            submit_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='submit']")
            
            logger.info(f"🔘 Botones encontrados: {len(buttons)}")
            logger.info(f"📤 Inputs submit encontrados: {len(submit_inputs)}")
            
            for i, button in enumerate(buttons):
                try:
                    button_text = button.text.strip()
                    button_type = button.get_attribute("type")
                    logger.info(f"  🔘 Botón {i+1}: text='{button_text}', type={button_type}")
                except:
                    pass
            
            for i, submit in enumerate(submit_inputs):
                try:
                    submit_value = submit.get_attribute("value")
                    submit_name = submit.get_attribute("name")
                    logger.info(f"  📤 Submit {i+1}: value='{submit_value}', name={submit_name}")
                except:
                    pass
            
            # Buscar enlaces
            links = self.driver.find_elements(By.TAG_NAME, "a")
            logger.info(f"🔗 Enlaces encontrados: {len(links)}")
            
            for i, link in enumerate(links[:10]):  # Solo los primeros 10
                try:
                    link_text = link.text.strip()
                    link_href = link.get_attribute("href")
                    if link_text and link_href:
                        logger.info(f"  🔗 Enlace {i+1}: '{link_text}' -> {link_href}")
                except:
                    pass
            
            # Buscar divs con clases específicas
            divs_with_class = self.driver.find_elements(By.CSS_SELECTOR, "div[class]")
            logger.info(f"📦 Divs con clase encontrados: {len(divs_with_class)}")
            
            # Mostrar algunas clases únicas
            unique_classes = set()
            for div in divs_with_class[:20]:  # Solo los primeros 20
                try:
                    class_name = div.get_attribute("class")
                    if class_name:
                        unique_classes.add(class_name)
                except:
                    pass
            
            logger.info(f"🏷️ Clases únicas encontradas (primeras 10): {list(unique_classes)[:10]}")
            
            logger.info("✅ Análisis de estructura completado")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error analizando estructura de la página: {str(e)}")
            return False
    
    def check_login_required(self):
        """Verifica si se requiere login para acceder a la página"""
        try:
            logger.info("🔐 Verificando si se requiere login...")
            
            # Buscar indicadores de login
            login_indicators = [
                "ingresá a tu cuenta",
                "olvidé mi contraseña",
                "mantener la sesión iniciada",
                "ingresar",
                "login",
                "sign in",
                "iniciar sesión"
            ]
            
            page_text = self.driver.find_element(By.TAG_NAME, "body").text.lower()
            
            login_required = False
            for indicator in login_indicators:
                if indicator in page_text:
                    login_required = True
                    logger.info(f"🔍 Indicador de login encontrado: '{indicator}'")
                    break
            
            if login_required:
                logger.warning("⚠️ Se requiere login para acceder a esta página")
                logger.info("📝 La página muestra un formulario de login")
            else:
                logger.info("✅ No se detectó formulario de login - página accesible directamente")
            
            return login_required
            
        except Exception as e:
            logger.error(f"❌ Error verificando login: {str(e)}")
            return False
    
    def perform_login(self):
        """
        Realiza el proceso de login en Movistar Arena simulando comportamiento humano
        
        Returns:
            bool: True si el login fue exitoso, False en caso contrario
        """
        try:
            logger.info("🔐 Iniciando proceso de login...")
            
            # Importar ActionChains para simular comportamiento humano
            from selenium.webdriver.common.action_chains import ActionChains
            actions = ActionChains(self.driver)
            
            # Buscar campo de email
            logger.info("🔍 Buscando campo de email...")
            email_field = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "inputEmail"))
            )
            logger.info("✅ Campo de email encontrado")
            
            # Buscar campo de contraseña
            logger.info("🔍 Buscando campo de contraseña...")
            password_field = self.driver.find_element(By.ID, "inputPassword")
            logger.info("✅ Campo de contraseña encontrado")
            
            # Esperar 5 segundos después de encontrar los campos
            logger.info("⏳ Esperando 5 segundos después de encontrar los campos...")
            time.sleep(5)
            
            # Simular comportamiento humano: mover mouse al campo de email
            logger.info("🖱️ Moviendo mouse al campo de email...")
            actions.move_to_element(email_field).pause(0.3).perform()
            
            # Limpiar y llenar campo de email con pausas humanas
            logger.info("✏️ Llenando campo de email...")
            email_field.clear()
            time.sleep(0.2)  # Pausa humana
            
            # Escribir email carácter por carácter como un humano
            email_text = "florencia.franco@daleplay.la"
            for char in email_text:
                email_field.send_keys(char)
                time.sleep(0.05)  # Pausa entre caracteres
            
            logger.info("✅ Email ingresado: florencia.franco@daleplay.la")
            
            # Pausa humana antes de ir al siguiente campo
            time.sleep(0.5)
            
            # Simular comportamiento humano: mover mouse al campo de contraseña
            logger.info("🖱️ Moviendo mouse al campo de contraseña...")
            actions.move_to_element(password_field).pause(0.3).perform()
            
            # Limpiar y llenar campo de contraseña con pausas humanas
            logger.info("✏️ Llenando campo de contraseña...")
            password_field.clear()
            time.sleep(0.2)  # Pausa humana
            
            # Escribir contraseña carácter por carácter como un humano
            password_text = "Salvador_1C"
            for char in password_text:
                password_field.send_keys(char)
                time.sleep(0.05)  # Pausa entre caracteres
            
            logger.info("✅ Contraseña ingresada")
            
            # Pausa humana antes de ir al botón
            time.sleep(0.5)
            
            # Buscar y hacer clic en el botón de login
            logger.info("🔍 Buscando botón de login...")
            login_button = self.driver.find_element(By.CLASS_NAME, "btn-login")
            logger.info("✅ Botón de login encontrado")
            
            # Simular comportamiento humano: mover mouse al botón y hacer clic
            logger.info("🖱️ Moviendo mouse al botón de login...")
            actions.move_to_element(login_button).pause(0.5).click().perform()
            logger.info("✅ Botón de login clickeado (simulando comportamiento humano)")
            
            # Esperar 5 segundos después del clic
            logger.info("⏳ Esperando 5 segundos después del login...")
            time.sleep(5)
            
            # Verificar si el login fue exitoso
            logger.info("🔍 Verificando si el login fue exitoso...")
            
            # Obtener la URL actual
            current_url = self.driver.current_url
            logger.info(f"🔗 URL actual después del login: {current_url}")
            
            # Contar divs con clase 'mud-select mud-autocomplete'
            divs = self.driver.find_elements(By.CSS_SELECTOR, "div.mud-select.mud-autocomplete")
            logger.info(f"🔢 Cantidad de divs con clase 'mud-select mud-autocomplete': {len(divs)}")
            print(f"DIVS ENCONTRADOS DESPUÉS DEL LOGIN: {len(divs)}")
            return True
                
        except Exception as e:
            logger.error(f"❌ Error durante el proceso de login: {str(e)}")
            return False
    
    def apply_advanced_evasion(self):
        """Applies advanced evasion techniques to mimic human behavior"""
        try:
            logger.info("🛡️ Aplicando técnicas avanzadas de evasión...")
            # Simulate random scrolling
            self.simulate_scrolling()
            # Simulate random window resizing
            self.simulate_window_resizing()
            logger.info("✅ Técnicas de evasión aplicadas exitosamente")
        except Exception as e:
            logger.error(f"❌ Error aplicando técnicas de evasión: {str(e)}")

    def simulate_mouse_movements(self):
        """Simulates random mouse movements to mimic human behavior"""
        try:
            logger.info("🖱️ Simulando movimientos de mouse...")
            actions = ActionChains(self.driver)
            for _ in range(random.randint(5, 10)):
                x_offset = random.randint(-100, 100)
                y_offset = random.randint(-100, 100)
                actions.move_by_offset(x_offset, y_offset).perform()
                time.sleep(random.uniform(0.1, 0.5))
            logger.info("✅ Movimientos de mouse simulados")
        except Exception as e:
            logger.error(f"❌ Error simulando movimientos de mouse: {str(e)}")

    def simulate_scrolling(self):
        """Simulates random scrolling to mimic human behavior"""
        try:
            logger.info("🖱️ Simulando desplazamiento...")
            for _ in range(random.randint(2, 5)):
                scroll_amount = random.randint(-300, 300)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(0.2, 0.6))
            logger.info("✅ Desplazamiento simulado")
        except Exception as e:
            logger.error(f"❌ Error simulando desplazamiento: {str(e)}")

    def simulate_window_resizing(self):
        """Simulates random window resizing to mimic human behavior"""
        try:
            logger.info("🖥️ Simulando cambio de tamaño de ventana...")
            for _ in range(random.randint(1, 3)):
                width = random.randint(800, 1200)
                height = random.randint(600, 900)
                self.driver.set_window_size(width, height)
                time.sleep(random.uniform(0.5, 1.0))
            logger.info("✅ Cambio de tamaño de ventana simulado")
        except Exception as e:
            logger.error(f"❌ Error simulando cambio de tamaño de ventana: {str(e)}")
    
    def interact_with_page_elements(self):
        """
        Interactúa con elementos específicos de la página después del login
        
        Returns:
            bool: True si se pudo completar la interacción, False en caso contrario
        """
        try:
            logger.info("🔍 Iniciando interacción con elementos específicos de la página...")
            
            # Esperar 2 segundos como primer paso
            logger.info("⏳ PASO 1: Esperando 2 segundos...")
            time.sleep(2)
            
            # Guardar los dos primeros inputs en variables
            first_input = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//input[@type='text']"))
            )
            second_input = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "(//input[@type='text'])[2]"))
            )
            
            # Hacer clic en el primer input y seleccionar la primera opción
            logger.info("🔍 PASO 2: Buscando el primer input...")
            logger.info("✅ Primer input encontrado - haciendo clic...")
            first_input.click()
            logger.info("✅ Input clickeado - esperando que se abran las opciones...")
            
            # Esperar un momento para que se abran las opciones
            time.sleep(2)
            
            # Buscar y logear todas las opciones disponibles
            logger.info("🔍 PASO 3: Buscando opciones disponibles...")
            
            # Intentar diferentes estrategias para encontrar las opciones
            options_found = []
            option_selectors = [
                "option",  # Elementos option estándar
                ".mud-list-item",  # Clases comunes de MudBlazor
                ".mud-select-item",  # Otra clase común
                "[role='option']",  # Elementos con role option
                ".mud-list-item-text",  # Texto de las opciones
                "li",  # Elementos de lista
                ".dropdown-item",  # Elementos de dropdown
                ".select-option"  # Opciones de select
            ]
            
            for selector in option_selectors:
                try:
                    options = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if options:
                        logger.info(f"📋 Opciones encontradas con selector '{selector}': {len(options)}")
                        for i, option in enumerate(options[:20]):  # Solo las primeras 20
                            try:
                                option_text = option.text.strip()
                                option_value = option.get_attribute("value")
                                option_class = option.get_attribute("class")
                                
                                if option_text:
                                    option_info = {
                                        "index": i + 1,
                                        "text": option_text,
                                        "value": option_value,
                                        "class": option_class
                                    }
                                    options_found.append(option_info)
                                    logger.info(f"  📋 Opción {i+1}: '{option_text}' (value: {option_value}, class: {option_class})")
                            except Exception as e:
                                logger.warning(f"⚠️ Error procesando opción {i+1}: {str(e)}")
                                continue
                        break  # Si encontramos opciones con este selector, no probamos los demás
                except Exception as e:
                    logger.debug(f"Selector '{selector}' no funcionó: {str(e)}")
                    continue
            
            # Si encontramos opciones, hacer clic en la primera
            if options_found:
                logger.info("🖱️ PASO 4: Haciendo clic en la primera opción...")
                try:
                    # Buscar la primera opción usando el selector que funcionó
                    first_option = self.driver.find_element(By.CSS_SELECTOR, ".mud-list-item:first-child")
                    logger.info(f"✅ Primera opción encontrada: '{first_option.text.strip()}'")
                    
                    # Simular comportamiento humano: mover el mouse y hacer clic
                    from selenium.webdriver.common.action_chains import ActionChains
                    actions = ActionChains(self.driver)
                    actions.move_to_element(first_option).pause(0.5).click().perform()
                    logger.info("✅ Primera opción clickeada (simulando comportamiento humano)")
                    
                    # Esperar 2 segundos
                    logger.info("⏳ PASO 5: Esperando 2 segundos...")
                    time.sleep(2)
                    
                    # Hacer clic en el segundo input
                    logger.info("🔍 PASO 6: Buscando el segundo input...")
                    logger.info("✅ Segundo input encontrado - haciendo clic...")
                    second_input.click()
                    logger.info("✅ Segundo input clickeado - esperando que se abran las opciones...")
                    
                    # Esperar un momento para que se abran las opciones
                    time.sleep(2)
                    
                    # Buscar y logear todas las opciones disponibles para el segundo input
                    logger.info("🔍 PASO 7: Buscando opciones disponibles para el segundo input...")
                    
                    options_found = []
                    for selector in option_selectors:
                        try:
                            options = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            if options:
                                logger.info(f"📋 Opciones encontradas con selector '{selector}': {len(options)}")
                                for i, option in enumerate(options[:20]):  # Solo las primeras 20
                                    try:
                                        option_text = option.text.strip()
                                        option_value = option.get_attribute("value")
                                        option_class = option.get_attribute("class")
                                        
                                        if option_text:
                                            option_info = {
                                                "index": i + 1,
                                                "text": option_text,
                                                "value": option_value,
                                                "class": option_class
                                            }
                                            options_found.append(option_info)
                                            logger.info(f"  📋 Opción {i+1}: '{option_text}' (value: {option_value}, class: {option_class})")
                                    except Exception as e:
                                        logger.warning(f"⚠️ Error procesando opción {i+1}: {str(e)}")
                                        continue
                                break  # Si encontramos opciones con este selector, no probamos los demás
                        except Exception as e:
                            logger.debug(f"Selector '{selector}' no funcionó: {str(e)}")
                            continue
                    
                    # Si encontramos opciones, hacer clic en la primera
                    if options_found:
                        logger.info("🖱️ PASO 8: Haciendo clic en la primera opción del segundo input...")
                        try:
                            # Buscar la primera opción usando el selector que funcionó
                            first_option = self.driver.find_element(By.CSS_SELECTOR, ".mud-list-item:first-child")
                            logger.info(f"✅ Primera opción del segundo input encontrada: '{first_option.text.strip()}'")
                            
                            # Simular comportamiento humano: mover el mouse y hacer clic
                            actions.move_to_element(first_option).pause(0.5).click().perform()
                            logger.info("✅ Primera opción del segundo input clickeada (simulando comportamiento humano)")
                            
                            # Esperar 2 segundos
                            logger.info("⏳ PASO 9: Esperando 2 segundos...")
                            time.sleep(2)
                            
                            # Buscar y hacer clic en el tercer input
                            logger.info("🔍 PASO 10: Buscando el tercer input...")
                            third_input = WebDriverWait(self.driver, 10).until(
                                EC.element_to_be_clickable((By.XPATH, "(//input[@type='text'])[3]"))
                            )
                            logger.info("✅ Tercer input encontrado - haciendo clic...")
                            
                            # Hacer clic en el tercer input
                            third_input.click()
                            logger.info("✅ Tercer input clickeado - esperando que se abran las opciones...")
                            
                            # Esperar un momento para que se abran las opciones
                            time.sleep(2)
                            
                            # Buscar y logear todas las opciones disponibles para el tercer input
                            logger.info("🔍 PASO 11: Buscando opciones disponibles para el tercer input...")
                            
                            options_found = []
                            for selector in option_selectors:
                                try:
                                    options = self.driver.find_elements(By.CSS_SELECTOR, selector)
                                    if options:
                                        logger.info(f"📋 Opciones encontradas con selector '{selector}': {len(options)}")
                                        for i, option in enumerate(options[:20]):  # Solo las primeras 20
                                            try:
                                                option_text = option.text.strip()
                                                option_value = option.get_attribute("value")
                                                option_class = option.get_attribute("class")
                                                
                                                if option_text:
                                                    option_info = {
                                                        "index": i + 1,
                                                        "text": option_text,
                                                        "value": option_value,
                                                        "class": option_class
                                                    }
                                                    options_found.append(option_info)
                                                    logger.info(f"  📋 Opción {i+1}: '{option_text}' (value: {option_value}, class: {option_class})")
                                            except Exception as e:
                                                logger.warning(f"⚠️ Error procesando opción {i+1}: {str(e)}")
                                                continue
                                        break  # Si encontramos opciones con este selector, no probamos los demás
                                except Exception as e:
                                    logger.debug(f"Selector '{selector}' no funcionó: {str(e)}")
                                    continue
                            
                            # Guardar solo los textos de las opciones para poder iterar
                            option_texts = [option['text'] for option in options_found]
                            total_options = len(option_texts)
                            logger.info(f"🔄 Iniciando iteración sobre {total_options} opciones del tercer input...")
                            
                            # Iterar sobre todas las opciones
                            for option_index in range(total_options):
                                try:
                                    logger.info(f"🖱️ Procesando opción {option_index+1}/{total_options}")
                                    
                                    # Intentar cerrar cualquier overlay que pueda estar abierto
                                    try:
                                        self.driver.execute_script("document.querySelectorAll('.mud-overlay').forEach(el => el.style.display = 'none');")
                                        time.sleep(0.5)
                                    except:
                                        pass
                                    
                                    # Hacer clic en el tercer input para abrir el dropdown nuevamente
                                    logger.info("🔍 Abriendo dropdown del tercer input...")
                                    self.driver.execute_script("arguments[0].click();", third_input)
                                    time.sleep(2)
                                    
                                    # Buscar todas las opciones disponibles en el dropdown recién abierto
                                    try:
                                        logger.info("🔍 Obteniendo opciones actuales del dropdown...")
                                        current_options = self.driver.find_elements(By.CSS_SELECTOR, ".mud-list-item")
                                        logger.info(f"📋 Opciones disponibles en el dropdown: {len(current_options)}")
                                        
                                        if option_index < len(current_options):
                                            current_option = current_options[option_index]
                                            option_text = current_option.text.strip()
                                            logger.info(f"🎯 Opción encontrada en índice {option_index}: '{option_text}'")
                                            
                                            # Hacer clic en la opción usando JavaScript
                                            self.driver.execute_script("arguments[0].click();", current_option)
                                            logger.info(f"✅ Opción '{option_text}' clickeada con JavaScript")
                                            
                                            # Esperar a que la página se actualice
                                            logger.info("⏳ Esperando que la página se actualice...")
                                            time.sleep(4)
                                            
                                            # Buscar spans de vendidos para esta opción
                                            try:
                                                logger.info("🔍 Buscando spans con información de vendidos...")
                                                vendidos_found = False
                                                
                                                # Buscar por texto 'Vendido' en toda la página
                                                spans = self.driver.find_elements(By.XPATH, "//span[contains(text(), 'Vendido')]")
                                                if spans:
                                                    for span in spans:
                                                        span_text = span.text.strip()
                                                        if 'Vendido' in span_text and span_text:
                                                            logger.info(f"📊 VENDIDOS ENCONTRADOS para '{option_text}': {span_text}")
                                                            print(f"VENDIDO - {option_text}: {span_text}")  # También imprimir en consola
                                                            vendidos_found = True
                                                
                                                # Buscar en contenedores específicos si no encontramos nada
                                                if not vendidos_found:
                                                    containers = self.driver.find_elements(By.CSS_SELECTOR, ".mud-paper, .mud-elevation-1, [class*='asientos'], [class*='info']")
                                                    for container in containers:
                                                        spans_in_container = container.find_elements(By.TAG_NAME, "span")
                                                        for span in spans_in_container:
                                                            span_text = span.text.strip()
                                                            if 'Vendido' in span_text and span_text:
                                                                logger.info(f"📊 VENDIDOS ENCONTRADOS para '{option_text}': {span_text}")
                                                                print(f"VENDIDO - {option_text}: {span_text}")  # También imprimir en consola
                                                                vendidos_found = True
                                                
                                                if not vendidos_found:
                                                    logger.info(f"⚠️ No se encontraron spans de 'Vendido' para la opción '{option_text}'")
                                                    print(f"SIN VENDIDOS - {option_text}: No se encontraron datos de vendidos")
                                            
                                            except Exception as e:
                                                logger.error(f"❌ Error buscando spans de vendidos para '{option_text}': {str(e)}")
                                    
                                    except Exception as e:
                                        logger.error(f"❌ Error obteniendo opciones del dropdown: {str(e)}")
                                        continue
                                    
                                    # Esperar un momento antes de pasar a la siguiente opción
                                    time.sleep(2)
                                    
                                except Exception as e:
                                    logger.error(f"❌ Error procesando opción en índice {option_index}: {str(e)}")
                                    continue
                            
                            logger.info("✅ Interacción con todas las opciones del tercer input completada")
                            
                        except Exception as e:
                            logger.error(f"❌ Error haciendo clic en la primera opción del segundo input: {str(e)}")
                            return False
                except Exception as e:
                    logger.error(f"❌ Error haciendo clic en la primera opción: {str(e)}")
                    return False
            
            logger.info("✅ Interacción con elementos de la página completada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en la interacción con elementos de la página: {str(e)}")
            return False
    
    def save_page_info_to_json(self):
        """Guarda información básica de la página en un archivo JSON"""
        try:
            logger.info("💾 Guardando información de la página...")
            
            page_info = {
                "scraped_at": datetime.now().isoformat(),
                "url": self.base_url,
                "title": self.driver.title,
                "current_url": self.driver.current_url,
                "page_source_length": len(self.driver.page_source),
                "body_text_preview": self.driver.find_element(By.TAG_NAME, "body").text[:1000]
            }
            
            # Crear nombre de archivo
            filename = f"pagina_movistar_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = os.path.join(self.download_folder, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(page_info, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ Información guardada en: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"❌ Error guardando información de la página: {str(e)}")
            return None
    
    def is_future_event(self, event_date_str):
        """
        MODIFICADO: Siempre retorna True para obtener TODOS los shows sin importar la fecha
        
        Args:
            event_date_str (str): Fecha del evento en formato string
            
        Returns:
            bool: Siempre True para incluir todos los eventos
        """
        logger.info(f"📅 PROCESANDO TODOS LOS EVENTOS (sin filtro de fecha): '{event_date_str}'")
        print(f"✅ PROCESANDO TODOS LOS EVENTOS: {event_date_str}")
        return True
    
    def extract_event_dates(self, soup):
        """
        Extrae las fechas de eventos de los divs con información de ventas por función
        
        Args:
            soup: Objeto BeautifulSoup con el HTML de la página
            
        Returns:
            list: Lista de fechas de eventos encontradas
        """
        try:
            logger.info("🔍 Buscando divs con información de ventas por función...")
            
            # Buscar divs con información de ventas (puede variar entre sector y función)
            # Primero intentar con VentasXSectorDTO (por si es compatible)
            ventas_divs = soup.find_all('div', {'titem': 'VentasXSectorDTO'})
            
            # Si no encuentra, buscar otros posibles selectores para ventas por función
            if not ventas_divs:
                # Intentar con posibles variantes para ventas por función
                possible_selectors = [
                    {'titem': 'VentasXFuncionDTO'},
                    {'titem': 'VentasPorFuncionDTO'}, 
                    {'titem': 'FuncionVentasDTO'},
                    {'class': 'ventas-funcion'},
                    {'class': 'function-sales'}
                ]
                
                for selector in possible_selectors:
                    ventas_divs = soup.find_all('div', selector)
                    if ventas_divs:
                        logger.info(f"📋 Encontrados divs con selector {selector}")
                        break
            
            logger.info(f"📋 Encontrados {len(ventas_divs)} divs con información de ventas")
            
            # Si no encuentra, intentar con otros atributos similares
            if not ventas_divs:
                logger.info("🔍 Intentando buscar con otros selectores...")
                
                # Buscar por cualquier atributo titem
                all_divs_with_titem = soup.find_all('div', attrs={'titem': True})
                logger.info(f"📋 Encontrados {len(all_divs_with_titem)} divs con cualquier titem")
                
                for div in all_divs_with_titem:
                    titem_value = div.get('titem', '')
                    logger.info(f"📋 titem encontrado: '{titem_value}'")
                    if 'VentasXSectorDTO' in titem_value:
                        ventas_divs.append(div)
                
                # También buscar por contenido que contenga VentasXSectorDTO
                divs_with_ventas_text = soup.find_all('div', string=lambda text: text and 'VentasXSectorDTO' in text)
                logger.info(f"📋 Divs con texto VentasXSectorDTO: {len(divs_with_ventas_text)}")
                
                # Buscar divs que contengan la palabra VentasXSectorDTO en cualquier lugar
                all_divs = soup.find_all('div')
                for div in all_divs:
                    div_str = str(div)
                    if 'VentasXSectorDTO' in div_str and div not in ventas_divs:
                        ventas_divs.append(div)
                        logger.info("📋 Encontrado div con VentasXSectorDTO en el HTML")
            
            logger.info(f"📋 Total de divs con VentasXSectorDTO encontrados: {len(ventas_divs)}")
            
            # IMPRIMIR TODO EL CONTENIDO DE LOS DIVS PARA DEBUGGEAR
            print(f"\n" + "="*80)
            print(f"🔍 DEBUG: CONTENIDO DE DIVS CON VentasXSectorDTO")
            print("="*80)
            
            for i, div in enumerate(ventas_divs):
                print(f"\n📋 DIV {i+1} COMPLETO:")
                print("-" * 50)
                
                # Imprimir atributos del div
                print(f"ATRIBUTOS: {div.attrs}")
                
                # Imprimir todo el HTML del div
                print(f"HTML COMPLETO:")
                print(div.prettify()[:1000] + "..." if len(str(div)) > 1000 else div.prettify())
                
                # Imprimir solo el texto
                print(f"TEXTO LIMPIO:")
                text_content = div.get_text(separator=' ').strip()
                print(text_content[:500] + "..." if len(text_content) > 500 else text_content)
                
                print("-" * 50)
            
            print("="*80)
            
            event_dates = []
            
            # Usar Selenium para extraer los valores reales de los inputs
            logger.info("🔍 Usando Selenium para extraer valores de inputs...")
            
            try:
                # Buscar todos los divs con titem="VentasXSectorDTO" usando Selenium
                selenium_divs = self.driver.find_elements(By.CSS_SELECTOR, 'div[titem="VentasXSectorDTO"]')
                logger.info(f"📋 Selenium encontró {len(selenium_divs)} divs con titem='VentasXSectorDTO'")
                
                for i, selenium_div in enumerate(selenium_divs):
                    try:
                        logger.info(f"🔍 Procesando div {i+1} con Selenium para extraer fecha...")
                        
                        # Buscar el input que contiene "Fecha evento" en su label
                        date_input = None
                        
                        # Buscar inputs dentro del div
                        inputs = selenium_div.find_elements(By.CSS_SELECTOR, 'input[type="text"]')
                        logger.info(f"📋 Encontrados {len(inputs)} inputs en el div {i+1}")
                        
                        for j, input_elem in enumerate(inputs):
                            try:
                                # Buscar el label asociado
                                labels = selenium_div.find_elements(By.XPATH, f'.//label[contains(text(), "Fecha evento")]')
                                if labels:
                                    # Si encontramos un label con "Fecha evento", buscar el input asociado
                                    for label in labels:
                                        # Obtener el for attribute del label
                                        for_attr = label.get_attribute("for")
                                        if for_attr:
                                            # Buscar input con ese id
                                            try:
                                                associated_input = selenium_div.find_element(By.CSS_SELECTOR, f'input[id="{for_attr}"]')
                                                date_input = associated_input
                                                logger.info(f"📅 Input de fecha encontrado por label en div {i+1}")
                                                break
                                            except:
                                                continue
                                        else:
                                            # Si no hay for, buscar el input más cercano
                                            try:
                                                # Buscar input que esté cerca del label
                                                parent = label.find_element(By.XPATH, './..')
                                                nearby_input = parent.find_element(By.CSS_SELECTOR, 'input[type="text"]')
                                                date_input = nearby_input
                                                logger.info(f"📅 Input de fecha encontrado por proximidad en div {i+1}")
                                                break
                                            except:
                                                continue
                                    
                                    if date_input:
                                        break
                                        
                                # Si no encontramos por label, intentar por posición (segundo input suele ser fecha)
                                if not date_input and j == 1:  # Segundo input (índice 1)
                                    date_input = input_elem
                                    logger.info(f"📅 Usando segundo input como fecha en div {i+1}")
                                    
                            except Exception as e:
                                logger.debug(f"Error procesando input {j+1}: {str(e)}")
                                continue
                        
                        # Extraer el valor del input de fecha
                        date_text = "Fecha no disponible"
                        if date_input:
                            try:
                                # Intentar diferentes métodos para obtener el valor
                                value = date_input.get_attribute("value")
                                if value and value.strip():
                                    date_text = value.strip()
                                    logger.info(f"📅 Valor extraído del input: '{date_text}'")
                                else:
                                    # Si no hay valor en value, intentar con texto visible
                                    text_content = date_input.text
                                    if text_content and text_content.strip():
                                        date_text = text_content.strip()
                                        logger.info(f"📅 Texto extraído del input: '{date_text}'")
                                    else:
                                        # Como último recurso, ejecutar JavaScript para obtener el valor
                                        js_value = self.driver.execute_script("return arguments[0].value;", date_input)
                                        if js_value and js_value.strip():
                                            date_text = js_value.strip()
                                            logger.info(f"📅 Valor extraído con JavaScript: '{date_text}'")
                                        else:
                                            logger.warning(f"⚠️ No se pudo extraer valor del input en div {i+1}")
                                            
                            except Exception as e:
                                logger.error(f"❌ Error extrayendo valor del input: {str(e)}")
                        
                        event_dates.append(date_text)
                        logger.info(f"✅ Fecha de evento {i+1} extraída: '{date_text}'")
                        print(f"📅 FECHA EVENTO {i+1}: {date_text}")
                        
                    except Exception as e:
                        logger.error(f"❌ Error procesando div {i+1}: {str(e)}")
                        event_dates.append("Error extrayendo fecha")
                        continue
                        
            except Exception as e:
                logger.error(f"❌ Error usando Selenium para extraer fechas: {str(e)}")
                # Fallback al método anterior con BeautifulSoup
                logger.info("🔄 Usando método fallback con BeautifulSoup...")
                for i, div in enumerate(ventas_divs):
                    event_dates.append("Fecha no disponible")
                    print(f"⚠️ DIV {i+1}: Fecha no disponible (fallback)")
            
            logger.info(f"📅 Total de fechas extraídas: {len(event_dates)}")
            return event_dates
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo fechas de eventos: {str(e)}")
            return []
    
    def extract_ventas_funcion_data_with_daily_breakdown(self, option_text):
        """
        Extrae datos de ventas por función con desglose diario haciendo clic en el botón expand
        
        Args:
            option_text (str): Texto de la opción que se está procesando
            
        Returns:
            list: Lista con los datos extraídos incluyendo desglose diario
        """
        try:
            logger.info("📊 Extrayendo datos de ventas por función con desglose diario...")
            
            # DEBUG específico para ERREWAY
            if "erreway" in option_text.lower() or "errewey" in option_text.lower():
                logger.info(f"🔍 DEBUG ERREWAY - Extrayendo datos para: '{option_text}'")
                logger.info(f"🔍 DEBUG ERREWAY - URL actual: {self.driver.current_url}")
                logger.info(f"🔍 DEBUG ERREWAY - Título de página: {self.driver.title}")
            
            # Primero extraer los datos principales (totales)
            main_data = self.extract_ventas_funcion_data_individual(option_text)
            if not main_data:
                if "erreway" in option_text.lower():
                    logger.error(f"❌ DEBUG ERREWAY - NO se pudieron extraer datos principales para: '{option_text}'")
                logger.warning("⚠️ No se pudieron extraer datos principales")
                
                # NUEVA LÓGICA: Crear registro con 0 ventas en lugar de saltarse el show
                logger.info(f"🔄 CREANDO REGISTRO CON 0 VENTAS para show activo: '{option_text}'")
                print(f"🔄 SHOW ACTIVO SIN VENTAS: Creando registro con 0 ventas para '{option_text}'")
                
                # Crear datos básicos con 0 ventas
                zero_sales_data = [{
                    'tabla_numero': 1,
                    'datos_ventas_funcion': [{
                        'fecha_evento': 'Sin fecha específica',
                        'vendido_total': 0,
                        'invitacion_total': 0,
                        'total_entradas': 0,
                        'recaudacion_total_ars': 0,
                        'disponible_total': 0,
                        'porcentaje_ocupacion': 0.0
                    }],
                    'desglose_diario': [],
                    'artista_procesado': option_text,
                    'fecha_extraccion': datetime.now(),
                    'tiene_ventas': False,  # Flag para identificar shows sin ventas
                    'motivo_cero_ventas': 'No se encontraron datos de ventas en el sistema'
                }]
                
                logger.info(f"✅ Registro con 0 ventas creado para: '{option_text}'")
                return zero_sales_data
            
            # DEBUG específico para ERREWAY - mostrar datos principales extraídos
            if "erreway" in option_text.lower() or "errewey" in option_text.lower():
                logger.info(f"✅ DEBUG ERREWAY - Datos principales extraídos: {len(main_data)} registros")
                for i, data in enumerate(main_data[:3]):  # Mostrar solo los primeros 3
                    logger.info(f"  DEBUG ERREWAY - Registro {i+1}: {data.get('evento', 'N/A')} - Vendidos: {data.get('vendidos_total', 'N/A')}")
            
            # Buscar el botón expand usando los selectores que proporcionaste
            expand_button = None
            selectors_to_try = [
                "body > div.mud-layout.mud-drawer-open-responsive-md-left.mud-drawer-left-clipped-never > div > div > div.mud-grid-item.mud-grid-item-xs-12.mb-4 > div > div > table > tbody > tr > td:nth-child(1) > button",
                "/html/body/div[3]/div/div/div[2]/div/div/table/tbody/tr/td[1]/button",
                "table.mud-table-root tbody tr td button[aria-label='expand']",
                "button[aria-label='expand']"
            ]
            
            for selector in selectors_to_try:
                try:
                    if selector.startswith("/"):
                        # XPath
                        expand_button = self.driver.find_element(By.XPATH, selector)
                    else:
                        # CSS Selector
                        expand_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if expand_button and expand_button.is_displayed():
                        logger.info(f"✅ Botón expand encontrado con selector: {selector}")
                        break
                except:
                    continue
            
            if not expand_button:
                logger.warning("⚠️ No se encontró el botón expand - retornando solo datos principales")
                return main_data
            
            # Hacer clic en el botón expand
            logger.info("🔽 Haciendo clic en el botón expand para mostrar desglose diario...")
            self.driver.execute_script("arguments[0].click();", expand_button)
            time.sleep(3)  # Esperar a que se despliegue la tabla
            
            # Extraer el desglose diario de la tabla desplegada
            daily_breakdown = self.extract_daily_breakdown_data()
            
            # Combinar datos principales con desglose diario
            if daily_breakdown:
                # Agregar desglose diario a los datos principales
                for data_item in main_data:
                    data_item['desglose_diario'] = daily_breakdown
                
                logger.info(f"✅ Desglose diario extraído: {len(daily_breakdown)} días")
                print(f"✅ DESGLOSE DIARIO: {len(daily_breakdown)} días de ventas")
            else:
                logger.warning("⚠️ No se pudo extraer el desglose diario")
                print("⚠️ SIN DESGLOSE DIARIO")
            
            return main_data
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos con desglose diario: {str(e)}")
            print(f"❌ ERROR DESGLOSE DIARIO: {str(e)}")
            return []

    def extract_multiple_events_with_daily_breakdown(self, option_text):
        """
        FUNCIÓN ESPECIAL PARA DUKI: Extrae desglose diario de MÚLTIPLES eventos
        Busca todos los botones "+" y extrae el desglose diario de cada evento
        
        Args:
            option_text (str): Texto de la opción que se está procesando
            
        Returns:
            list: Lista con datos de cada evento con su desglose diario
        """
        try:
            logger.info("🎯 FUNCIÓN ESPECIAL DUKI: Extrayendo desglose diario de múltiples eventos...")
            print("🎯 FUNCIÓN ESPECIAL DUKI: Procesando múltiples eventos con desglose diario")
            
            # Primero extraer los datos principales (resumen de todos los eventos) 
            # NOTA: extract_ventas_funcion_data_individual devuelve TODOS los eventos en una lista
            raw_data = self.extract_ventas_funcion_data_individual(option_text)
            if not raw_data:
                logger.warning("⚠️ No se pudieron extraer datos principales")
                
                # NUEVA LÓGICA: Crear registro con 0 ventas para función especial DUKI
                logger.info(f"🔄 FUNCIÓN DUKI - CREANDO REGISTRO CON 0 VENTAS para show activo: '{option_text}'")
                print(f"🔄 DUKI SIN VENTAS: Creando registro con 0 ventas para '{option_text}'")
                
                # Crear datos básicos con 0 ventas para DUKI
                zero_sales_data = [{
                    'tabla_numero': 1,
                    'datos_ventas_funcion': [{
                        'fecha_evento': 'Sin fecha específica',
                        'vendido_total': 0,
                        'invitacion_total': 0,
                        'total_entradas': 0,
                        'recaudacion_total_ars': 0,
                        'disponible_total': 0,
                        'porcentaje_ocupacion': 0.0
                    }],
                    'desglose_diario': [],
                    'artista_procesado': option_text,
                    'fecha_extraccion': datetime.now(),
                    'tiene_ventas': False,  # Flag para identificar shows sin ventas
                    'motivo_cero_ventas': 'DUKI - No se encontraron datos de ventas en el sistema'
                }]
                
                logger.info(f"✅ DUKI - Registro con 0 ventas creado para: '{option_text}'")
                return zero_sales_data
            
            # Los datos vienen como UNA lista con todos los eventos, necesitamos separarlos
            main_data = raw_data[0]['datos_ventas_funcion'] if raw_data and raw_data[0].get('datos_ventas_funcion') else []
            
            logger.info(f"✅ Datos principales extraídos: {len(main_data)} eventos")
            print(f"📊 DATOS PRINCIPALES: {len(main_data)} eventos encontrados")
            
            # Buscar TODOS los botones expand (uno por cada fila/evento)
            expand_buttons = []
            selectors_to_try = [
                "table.mud-table-root tbody tr td button",
                "tbody tr td:nth-child(1) button",
                "button[aria-label='expand']",
                ".mud-table-body tr td button"
            ]
            
            for selector in selectors_to_try:
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if buttons:
                        expand_buttons = buttons
                        logger.info(f"✅ Encontrados {len(buttons)} botones expand con selector: {selector}")
                        break
                except:
                    continue
            
            if not expand_buttons:
                logger.warning("⚠️ No se encontraron botones expand - retornando solo datos principales")
                return main_data
            
            logger.info(f"🔍 Procesando {len(expand_buttons)} botones expand para extraer desglose diario...")
            print(f"🔍 PROCESANDO {len(expand_buttons)} EVENTOS CON DESGLOSE DIARIO")
            
            # Procesar cada botón expand (cada evento)
            eventos_con_desglose = []
            for i, button in enumerate(expand_buttons):
                try:
                    # EXTRAER LA FECHA DIRECTAMENTE DE LA FILA DE LA TABLA
                    # Encontrar la fila que contiene este botón expand
                    fila_elemento = button.find_element(By.XPATH, "./ancestor::tr")
                    
                    # Extraer la fecha de la primera columna de esta fila
                    columnas = fila_elemento.find_elements(By.TAG_NAME, "td")
                    if len(columnas) >= 1:
                        # La fecha está en la primera columna (antes del botón)
                        fecha_fila = columnas[0].text.strip()
                        logger.info(f"📅 Fecha extraída de la fila {i+1}: '{fecha_fila}'")
                        # Si la primera columna está vacía, intentar con la segunda
                        if not fecha_fila and len(columnas) >= 2:
                            fecha_fila = columnas[1].text.strip()
                            logger.info(f"📅 Fecha extraída de la segunda columna {i+1}: '{fecha_fila}'")
                    else:
                        fecha_fila = f"Evento {i+1}"
                        logger.warning(f"⚠️ No se pudo extraer fecha de la fila {i+1}")
                    
                    # Crear datos del evento con la fecha correcta
                    evento_data = {
                        'fecha_evento': fecha_fila,
                        'artista': option_text,
                        'recaudacion_ars': 0,  # Se actualizará con el desglose
                        'cantidad_tickets': 0  # Se actualizará con el desglose
                    }
                    
                    logger.info(f"🔽 Expandiendo evento {i+1}/{len(expand_buttons)}: {fecha_fila}")
                    print(f"🔽 EXPANDIENDO EVENTO {i+1}: {fecha_fila}")
                    
                    # Hacer clic en el botón expand
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(3)  # Esperar a que se despliegue
                    
                    # Extraer el desglose diario de este evento específico
                    daily_breakdown = self.extract_daily_breakdown_data()
                    
                    if daily_breakdown:
                        # Agregar desglose diario a los datos del evento
                        evento_data['desglose_diario'] = daily_breakdown
                        logger.info(f"✅ Evento {i+1}: {len(daily_breakdown)} días de desglose diario")
                        print(f"✅ EVENTO {i+1}: {len(daily_breakdown)} días de ventas")
                    else:
                        logger.warning(f"⚠️ Evento {i+1}: Sin desglose diario")
                        print(f"⚠️ EVENTO {i+1}: Sin desglose diario")
                    
                    # Agregar a la lista de eventos procesados
                    eventos_con_desglose.append(evento_data)
                    
                    # Colapsar el desglose antes del siguiente (clickear otra vez el botón)
                    try:
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(1)
                        logger.info(f"🔼 Evento {i+1} colapsado")
                    except:
                        logger.warning(f"⚠️ No se pudo colapsar evento {i+1}")
                    
                except Exception as e:
                    logger.error(f"❌ Error procesando evento {i+1}: {str(e)}")
                    # Si hay error, crear evento básico
                    evento_basico = {
                        'fecha_evento': f"Evento {i+1}",
                        'artista': option_text,
                        'recaudacion_ars': 0,
                        'cantidad_tickets': 0
                    }
                    eventos_con_desglose.append(evento_basico)
                    continue
            
            # Convertir cada evento en una tabla separada
            tablas_resultado = []
            for i, evento in enumerate(eventos_con_desglose):
                tabla = {
                    'tabla_numero': i + 1,
                    'dataframe': None,
                    'filas': 1,
                    'columnas': 4,
                    'fecha_evento': evento['fecha_evento'],
                    'datos_ventas_funcion': [evento]  # Cada evento en su propia tabla
                }
                tablas_resultado.append(tabla)
            
            logger.info(f"🎉 FUNCIÓN ESPECIAL DUKI COMPLETADA: {len(tablas_resultado)} eventos con desglose diario")
            print(f"🎉 DUKI COMPLETADO: {len(tablas_resultado)} eventos procesados")
            return tablas_resultado
            
        except Exception as e:
            logger.error(f"❌ Error en función especial DUKI: {str(e)}")
            print(f"❌ ERROR FUNCIÓN DUKI: {str(e)}")
            return []

    def extract_daily_breakdown_data(self):
        """
        Extrae los datos del desglose diario de la tabla desplegada
        
        Returns:
            list: Lista con el desglose diario
        """
        try:
            logger.info("📅 Extrayendo desglose diario...")
            
            # Buscar la tabla desplegada con el desglose diario
            daily_table_selectors = [
                "body > div.mud-layout.mud-drawer-open-responsive-md-left.mud-drawer-left-clipped-never > div > div > div.mud-grid-item.mud-grid-item-xs-12.mb-4 > div > div > table > tbody > tr:nth-child(2) > td > div > div > table > tbody",
                "/html/body/div[3]/div/div/div[2]/div/div/table/tbody/tr[2]/td/div/div/table/tbody"
            ]
            
            daily_tbody = None
            for selector in daily_table_selectors:
                try:
                    if selector.startswith("/"):
                        daily_tbody = self.driver.find_element(By.XPATH, selector)
                    else:
                        daily_tbody = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if daily_tbody:
                        logger.info(f"✅ Tabla de desglose diario encontrada con: {selector}")
                        break
                except:
                    continue
            
            if not daily_tbody:
                logger.warning("⚠️ No se encontró la tabla de desglose diario")
                return []
            
            # Extraer las filas del desglose diario
            daily_rows = daily_tbody.find_elements(By.CSS_SELECTOR, "tr.mud-table-row")
            logger.info(f"📋 Encontradas {len(daily_rows)} filas de desglose diario")
            
            daily_data = []
            for i, row in enumerate(daily_rows):
                try:
                    cells = row.find_elements(By.CSS_SELECTOR, "td.mud-table-cell")
                    
                    # Estructura esperada: [columna_vacia], fecha, cantidad, monto
                    if len(cells) >= 3:
                        # Saltar la primera celda si está vacía o es rowspan
                        fecha_cell_idx = 1 if len(cells) >= 4 else 0
                        
                        fecha = cells[fecha_cell_idx].text.strip()
                        cantidad = cells[fecha_cell_idx + 1].text.strip()
                        monto = cells[fecha_cell_idx + 2].text.strip()
                        
                        if fecha and cantidad and monto:
                            # Limpiar datos
                            cantidad_limpia = cantidad.replace('.', '').replace(',', '')
                            monto_limpio = monto.replace('$', '').strip()
                            
                            daily_item = {
                                'fecha_venta': fecha,
                                'cantidad_vendida': cantidad_limpia,
                                'monto_vendido': monto_limpio
                            }
                            
                            daily_data.append(daily_item)
                            
                            logger.info(f"✅ Día {i+1}: {fecha} - {cantidad_limpia} tickets - ${monto_limpio}")
                            print(f"📅 DÍA {i+1}: {fecha} | {cantidad_limpia} tickets | ${monto_limpio}")
                
                except Exception as e:
                    logger.debug(f"Error procesando fila diaria {i+1}: {str(e)}")
                    continue
            
            logger.info(f"✅ Desglose diario extraído: {len(daily_data)} días")
            return daily_data
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo desglose diario: {str(e)}")
            return []

    def extract_ventas_funcion_data_individual(self, option_text):
        """
        Extrae datos específicos de ventas por función de la tabla usando los selectores exactos
        
        Args:
            option_text (str): Texto de la opción que se está procesando
            
        Returns:
            list: Lista con los datos extraídos en formato compatible
        """
        try:
            logger.info("📊 Extrayendo datos de ventas por función con selectores exactos...")
            
            # Intentar encontrar la tabla con reintentos
            table_selectors = [
                ("/html/body/div[3]/div/div/div[2]/div/div/table", "XPath exacto"),
                ("body > div.mud-layout.mud-drawer-open-responsive-md-left.mud-drawer-left-clipped-never > div > div > div.mud-grid-item.mud-grid-item-xs-12.mb-4 > div > div > table", "CSS selector completo"),
                ("table.mud-table-root", "CSS selector genérico")
            ]
            
            table_element = None
            max_attempts = 3
            wait_seconds = 10
            
            for attempt in range(max_attempts):
                logger.info(f"🔄 Intento {attempt + 1}/{max_attempts} de encontrar la tabla...")
                
                for selector, description in table_selectors:
                    try:
                        if selector.startswith("/"):
                            # XPath
                            table_element = self.driver.find_element(By.XPATH, selector)
                        else:
                            # CSS Selector
                            table_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        if table_element:
                            logger.info(f"✅ Tabla encontrada con {description} (intento {attempt + 1})")
                            break
                            
                    except Exception as e:
                        logger.warning(f"⚠️ No se encontró tabla con {description} (intento {attempt + 1})")
                        continue
                
                # Si encontramos la tabla, salir del bucle de reintentos
                if table_element:
                    break
                    
                # Si no es el último intento, esperar antes de reintentar
                if attempt < max_attempts - 1:
                    logger.info(f"⏳ Esperando {wait_seconds} segundos antes del siguiente intento...")
                    print(f"⏳ ESPERANDO {wait_seconds}s - Intento {attempt + 1}/{max_attempts}")
                    time.sleep(wait_seconds)
            
            if not table_element:
                logger.error(f"❌ No se pudo encontrar la tabla después de {max_attempts} intentos")
                return []
            
            # Buscar las filas del tbody
            tbody_rows = table_element.find_elements(By.CSS_SELECTOR, "tbody.mud-table-body tr.mud-table-row")
            logger.info(f"📋 Encontradas {len(tbody_rows)} filas de datos en tbody")
            
            extracted_data = []
            
            for i, row in enumerate(tbody_rows):
                try:
                    # Buscar las celdas específicas
                    cells = row.find_elements(By.CSS_SELECTOR, "td.mud-table-cell")
                    
                    if len(cells) >= 4:
                        # Extraer datos según la estructura que mostraste:
                        # Celda 0: Fecha Función (con botón expand)
                        # Celda 1: Fecha de Venta (vacía)
                        # Celda 2: Cantidad de Tickets (text-right-no-wrap bold)
                        # Celda 3: Recaudación (text-right-no-wrap bold)
                        
                        fecha_funcion_raw = cells[0].text.strip()
                        fecha_venta = cells[1].text.strip()
                        cantidad_tickets = cells[2].text.strip()
                        recaudacion = cells[3].text.strip()
                        
                        # Limpiar fecha función (quitar texto del botón)
                        fecha_funcion = fecha_funcion_raw.split('\n')[0] if '\n' in fecha_funcion_raw else fecha_funcion_raw
                        fecha_funcion = fecha_funcion.replace('expand', '').strip()
                        
                        # Validar que tenemos datos válidos
                        if fecha_funcion and cantidad_tickets and recaudacion:
                            # Limpiar cantidad de tickets (quitar puntos)
                            cantidad_limpia = cantidad_tickets.replace('.', '').replace(',', '')
                            
                            # Limpiar recaudación (quitar $ y espacios)
                            recaudacion_limpia = recaudacion.replace('$', '').strip()
                            
                            data_row = {
                                'fecha_funcion': fecha_funcion,
                                'fecha_venta': fecha_venta if fecha_venta else 'N/A',
                                'cantidad_tickets': cantidad_limpia,
                                'recaudacion_ars': recaudacion_limpia,
                                'artista': option_text
                            }
                            
                            extracted_data.append(data_row)
                            
                            logger.info(f"✅ Fila {i+1} extraída:")
                            logger.info(f"  📅 Fecha Función: {fecha_funcion}")
                            logger.info(f"  📅 Fecha Venta: {fecha_venta if fecha_venta else 'N/A'}")
                            logger.info(f"  🎫 Cantidad Tickets: {cantidad_limpia}")
                            logger.info(f"  💰 Recaudación: {recaudacion_limpia}")
                            
                            print(f"📊 DATOS EXTRAÍDOS - Fila {i+1}:")
                            print(f"  📅 Fecha Función: {fecha_funcion}")
                            print(f"  📅 Fecha Venta: {fecha_venta if fecha_venta else 'N/A'}")
                            print(f"  🎫 Cantidad Tickets: {cantidad_limpia}")
                            print(f"  💰 Recaudación ARS: {recaudacion_limpia}")
                            print(f"  🎤 Artista: {option_text}")
                            print("-" * 60)
                        
                except Exception as e:
                    logger.debug(f"Error procesando fila {i+1}: {str(e)}")
                    continue
            
            if extracted_data:
                logger.info(f"✅ Extracción completada: {len(extracted_data)} filas de datos")
                print(f"✅ EXTRACCIÓN COMPLETADA: {len(extracted_data)} filas para {option_text}")
                
                # Convertir a formato compatible
                compatible_data = [{
                    'tabla_numero': 1,
                    'dataframe': None,
                    'filas': len(extracted_data),
                    'columnas': 4,
                    'fecha_evento': extracted_data[0]['fecha_funcion'] if extracted_data else 'Fecha no disponible',
                    'datos_ventas_funcion': extracted_data
                }]
                
                return compatible_data
            else:
                logger.warning("⚠️ No se pudieron extraer datos de la tabla")
                print(f"⚠️ SIN DATOS EXTRAÍDOS para {option_text}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos de ventas por función: {str(e)}")
            print(f"❌ ERROR EXTRAYENDO DATOS para {option_text}: {str(e)}")
            return []

    def extract_ventas_funcion_data(self, option_text):
        """
        Extrae datos específicos de ventas por función de la tabla
        
        Args:
            option_text (str): Texto de la opción que se está procesando
            
        Returns:
            list: Lista con los datos extraídos en formato compatible
        """
        try:
            logger.info("📊 Extrayendo datos de ventas por función...")
            
            # Buscar las filas de la tabla que contienen los datos
            table_rows = self.driver.find_elements(By.CSS_SELECTOR, "tr.mud-table-row")
            logger.info(f"📋 Encontradas {len(table_rows)} filas en la tabla")
            
            extracted_data = []
            
            for i, row in enumerate(table_rows):
                try:
                    # Buscar las celdas de la fila
                    cells = row.find_elements(By.CSS_SELECTOR, "td.mud-table-cell")
                    
                    if len(cells) >= 4:  # Necesitamos al menos 4 celdas
                        # Extraer datos de las celdas
                        fecha_cell = cells[0].text.strip() if len(cells) > 0 else ""
                        # Celda 1 suele estar vacía
                        venta_total_cell = cells[2].text.strip() if len(cells) > 2 else ""
                        monto_total_cell = cells[3].text.strip() if len(cells) > 3 else ""
                        
                        # Limpiar y validar los datos
                        if fecha_cell and venta_total_cell and monto_total_cell:
                            # Limpiar fecha (quitar botones y elementos extra)
                            fecha_limpia = fecha_cell.split('\n')[0] if '\n' in fecha_cell else fecha_cell
                            
                            # Limpiar venta total (quitar puntos y comas, convertir a número)
                            venta_total_limpia = venta_total_cell.replace('.', '').replace(',', '')
                            
                            # Limpiar monto total (quitar $ y espacios, mantener formato)
                            monto_total_limpio = monto_total_cell.replace('$', '').strip()
                            
                            data_row = {
                                'fecha_evento': fecha_limpia,
                                'venta_total': venta_total_limpia,
                                'monto_total_ars': monto_total_limpio,
                                'artista': option_text
                            }
                            
                            extracted_data.append(data_row)
                            
                            logger.info(f"✅ Fila {i+1} extraída:")
                            logger.info(f"  📅 Fecha: {fecha_limpia}")
                            logger.info(f"  🎫 Venta total: {venta_total_limpia}")
                            logger.info(f"  💰 Monto total: {monto_total_limpio}")
                            
                            print(f"📊 DATOS EXTRAÍDOS - Fila {i+1}:")
                            print(f"  📅 Fecha: {fecha_limpia}")
                            print(f"  🎫 Venta Total: {venta_total_limpia}")
                            print(f"  💰 Monto Total: {monto_total_limpio}")
                            print(f"  🎤 Artista: {option_text}")
                            print("-" * 50)
                        
                except Exception as e:
                    logger.debug(f"Error procesando fila {i+1}: {str(e)}")
                    continue
            
            if extracted_data:
                logger.info(f"✅ Extracción completada: {len(extracted_data)} filas de datos")
                print(f"✅ EXTRACCIÓN COMPLETADA: {len(extracted_data)} filas para {option_text}")
                
                # Convertir a formato compatible con el sistema existente
                # CREAR UNA TABLA POR CADA EVENTO (no todos juntos)
                compatible_data = []
                
                for i, evento_data in enumerate(extracted_data):
                    # Saltar la fila "Total" si existe
                    if evento_data.get('fecha_evento') == 'Total':
                        continue
                        
                    tabla_individual = {
                        'tabla_numero': i + 1,
                        'dataframe': None,  # No necesitamos DataFrame para este caso
                        'filas': 1,  # Una fila por tabla (un evento por tabla)
                        'columnas': 4,
                        'fecha_evento': evento_data['fecha_evento'],
                        'datos_ventas_funcion': [evento_data]  # Solo este evento
                    }
                    compatible_data.append(tabla_individual)
                
                logger.info(f"🎯 Creadas {len(compatible_data)} tablas individuales para {len(compatible_data)} eventos")
                return compatible_data
            else:
                logger.warning("⚠️ No se pudieron extraer datos de la tabla")
                print(f"⚠️ SIN DATOS EXTRAÍDOS para {option_text}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos de ventas por función: {str(e)}")
            print(f"❌ ERROR EXTRAYENDO DATOS para {option_text}: {str(e)}")
            return []

    def extract_tables_only(self):
        """Extrae todas las tablas de la página y las retorna sin guardarlas"""
        try:
            logger.info("🔍 Buscando tablas en la página...")
            
            # Obtener el HTML de la página
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Buscar divs con información de fechas de eventos
            logger.info("📅 Buscando fechas de eventos...")
            event_dates = self.extract_event_dates(soup)
            
            # Buscar todas las tablas
            tables = soup.find_all('table')
            logger.info(f"📊 Encontradas {len(tables)} tablas en la página")
            
            if not tables:
                logger.info("⚠️ No se encontraron tablas en la página")
                return []
            
            dataframes = []
            
            # Procesar cada tabla
            for i, table in enumerate(tables):
                try:
                    logger.info(f"📋 Procesando tabla {i+1}/{len(tables)}...")
                    
                    # Extraer headers
                    headers = []
                    thead = table.find('thead')
                    if thead:
                        header_row = thead.find('tr')
                        if header_row:
                            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
                    
                    # Si no hay headers en thead, buscar en la primera fila del tbody
                    if not headers:
                        tbody = table.find('tbody')
                        if tbody:
                            first_row = tbody.find('tr')
                            if first_row:
                                headers = [th.get_text(strip=True) for th in first_row.find_all(['th', 'td'])]
                    
                    # Extraer datos de las filas
                    rows_data = []
                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        # Si la primera fila son headers, saltarla
                        start_row = 1 if not thead and headers else 0
                        
                        for row in rows[start_row:]:
                            cells = row.find_all(['td', 'th'])
                            row_data = [cell.get_text(strip=True) for cell in cells]
                            if row_data:  # Solo agregar filas que no estén vacías
                                rows_data.append(row_data)
                    else:
                        # Si no hay tbody, buscar filas directamente en la tabla
                        rows = table.find_all('tr')
                        start_row = 1 if headers else 0
                        
                        for row in rows[start_row:]:
                            cells = row.find_all(['td', 'th'])
                            row_data = [cell.get_text(strip=True) for cell in cells]
                            if row_data:
                                rows_data.append(row_data)
                    
                    # Crear DataFrame
                    if rows_data:
                        # Asegurar que todas las filas tengan la misma longitud
                        max_cols = max(len(row) for row in rows_data) if rows_data else 0
                        
                        # Si no hay headers, crear headers genéricos
                        if not headers:
                            headers = [f"Columna_{j+1}" for j in range(max_cols)]
                        elif len(headers) < max_cols:
                            # Agregar headers adicionales si faltan
                            headers.extend([f"Columna_{j+1}" for j in range(len(headers), max_cols)])
                        
                        # Normalizar filas para que tengan la misma longitud
                        normalized_rows = []
                        for row in rows_data:
                            normalized_row = row + [''] * (max_cols - len(row))
                            normalized_rows.append(normalized_row[:max_cols])  # Truncar si es muy larga
                        
                        # Crear DataFrame
                        df = pd.DataFrame(normalized_rows, columns=headers[:max_cols])
                        
                        # Limpiar DataFrame (remover filas completamente vacías)
                        df = df.dropna(how='all')
                        
                        if not df.empty:
                            # Obtener la fecha correspondiente a esta tabla
                            event_date = event_dates[i] if i < len(event_dates) else "Fecha no disponible"
                            
                            # Solo incluir tablas de eventos futuros
                            if self.is_future_event(event_date):
                                dataframes.append({
                                    'tabla_numero': i + 1,
                                    'dataframe': df,
                                    'filas': len(df),
                                    'columnas': len(df.columns),
                                    'fecha_evento': event_date
                                })
                                
                                logger.info(f"✅ Tabla {i+1} procesada: {len(df)} filas x {len(df.columns)} columnas (evento futuro)")
                            else:
                                logger.info(f"❌ Tabla {i+1} excluida: evento pasado")
                        else:
                            logger.info(f"⚠️ Tabla {i+1} está vacía después del procesamiento")
                    else:
                        logger.info(f"⚠️ Tabla {i+1} no contiene datos")
                        
                except Exception as e:
                    logger.error(f"❌ Error procesando tabla {i+1}: {str(e)}")
                    continue
            
            # Resumen final
            logger.info(f"📊 RESUMEN FINAL:")
            logger.info(f"  📋 Total de tablas encontradas: {len(tables)}")
            logger.info(f"  ✅ Tablas de eventos futuros: {len(dataframes)}")
            
            return dataframes
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo tablas: {str(e)}")
            return []
    
    def extract_and_save_tables(self, artist_name=None):
        """Extrae todas las tablas de la página y las guarda en DataFrames y JSON"""
        try:
            logger.info("🔍 Buscando tablas en la página...")
            
            # Obtener el HTML de la página
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Buscar divs con información de fechas de eventos
            logger.info("📅 Buscando fechas de eventos...")
            event_dates = self.extract_event_dates(soup)
            
            # Buscar todas las tablas
            tables = soup.find_all('table')
            logger.info(f"📊 Encontradas {len(tables)} tablas en la página")
            
            if not tables:
                logger.info("⚠️ No se encontraron tablas en la página")
                return
            
            dataframes = []
            
            # Procesar cada tabla
            for i, table in enumerate(tables):
                try:
                    logger.info(f"📋 Procesando tabla {i+1}/{len(tables)}...")
                    
                    # Extraer headers
                    headers = []
                    thead = table.find('thead')
                    if thead:
                        header_row = thead.find('tr')
                        if header_row:
                            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
                    
                    # Si no hay headers en thead, buscar en la primera fila del tbody
                    if not headers:
                        tbody = table.find('tbody')
                        if tbody:
                            first_row = tbody.find('tr')
                            if first_row:
                                headers = [th.get_text(strip=True) for th in first_row.find_all(['th', 'td'])]
                    
                    # Extraer datos de las filas
                    rows_data = []
                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        # Si la primera fila son headers, saltarla
                        start_row = 1 if not thead and headers else 0
                        
                        for row in rows[start_row:]:
                            cells = row.find_all(['td', 'th'])
                            row_data = [cell.get_text(strip=True) for cell in cells]
                            if row_data:  # Solo agregar filas que no estén vacías
                                rows_data.append(row_data)
                    else:
                        # Si no hay tbody, buscar filas directamente en la tabla
                        rows = table.find_all('tr')
                        start_row = 1 if headers else 0
                        
                        for row in rows[start_row:]:
                            cells = row.find_all(['td', 'th'])
                            row_data = [cell.get_text(strip=True) for cell in cells]
                            if row_data:
                                rows_data.append(row_data)
                    
                    # Crear DataFrame
                    if rows_data:
                        # Asegurar que todas las filas tengan la misma longitud
                        max_cols = max(len(row) for row in rows_data) if rows_data else 0
                        
                        # Si no hay headers, crear headers genéricos
                        if not headers:
                            headers = [f"Columna_{j+1}" for j in range(max_cols)]
                        elif len(headers) < max_cols:
                            # Agregar headers adicionales si faltan
                            headers.extend([f"Columna_{j+1}" for j in range(len(headers), max_cols)])
                        
                        # Normalizar filas para que tengan la misma longitud
                        normalized_rows = []
                        for row in rows_data:
                            normalized_row = row + [''] * (max_cols - len(row))
                            normalized_rows.append(normalized_row[:max_cols])  # Truncar si es muy larga
                        
                        # Crear DataFrame
                        df = pd.DataFrame(normalized_rows, columns=headers[:max_cols])
                        
                        # Limpiar DataFrame (remover filas completamente vacías)
                        df = df.dropna(how='all')
                        
                        if not df.empty:
                            # Obtener la fecha correspondiente a esta tabla
                            event_date = event_dates[i] if i < len(event_dates) else "Fecha no disponible"
                            
                            dataframes.append({
                                'tabla_numero': i + 1,
                                'dataframe': df,
                                'filas': len(df),
                                'columnas': len(df.columns),
                                'fecha_evento': event_date
                            })
                            
                            logger.info(f"✅ Tabla {i+1} procesada: {len(df)} filas x {len(df.columns)} columnas")
                            print(f"\n" + "="*60)
                            print(f"📅 FECHA EVENTO: {event_date}")
                            print("="*60)
                            print(f"TABLA {i+1}")
                            print("="*60)
                            print(df.to_string(index=False))
                            print(f"\nRESUMEN TABLA {i+1}:")
                            print(f"- Fecha evento: {event_date}")
                            print(f"- Filas: {len(df)}")
                            print(f"- Columnas: {len(df.columns)}")
                            print(f"- Columnas: {list(df.columns)}")
                        else:
                            logger.info(f"⚠️ Tabla {i+1} está vacía después del procesamiento")
                    else:
                        logger.info(f"⚠️ Tabla {i+1} no contiene datos")
                        
                except Exception as e:
                    logger.error(f"❌ Error procesando tabla {i+1}: {str(e)}")
                    continue
            
            # Resumen final
            logger.info(f"📊 RESUMEN FINAL:")
            logger.info(f"  📋 Total de tablas encontradas: {len(tables)}")
            logger.info(f"  ✅ Tablas procesadas exitosamente: {len(dataframes)}")
            
            if dataframes:
                print(f"\n" + "="*60)
                print("RESUMEN DE TODAS LAS TABLAS EXTRAÍDAS")
                print("="*60)
                
                for table_info in dataframes:
                    print(f"Tabla {table_info['tabla_numero']}: {table_info['filas']} filas x {table_info['columnas']} columnas")
                
                # Guardar DataFrames como atributo de la clase para acceso posterior
                self.extracted_tables = dataframes
                logger.info("✅ Tablas guardadas en self.extracted_tables")
                
                # Guardar en JSON si se proporcionó nombre del artista Y hay eventos futuros
                if artist_name:
                    # Verificar si hay al menos un evento futuro
                    has_future_events = False
                    future_tables = []
                    
                    for table_info in dataframes:
                        event_date = table_info.get('fecha_evento', 'Fecha no disponible')
                        if self.is_future_event(event_date):
                            has_future_events = True
                            future_tables.append(table_info)
                            logger.info(f"✅ Tabla {table_info['tabla_numero']} incluida: evento futuro")
                        else:
                            logger.info(f"❌ Tabla {table_info['tabla_numero']} excluida: evento pasado")
                    
                    if has_future_events:
                        logger.info(f"🎯 Creando JSON con {len(future_tables)} tablas de eventos futuros")
                        print(f"💾 CREANDO JSON: {len(future_tables)}/{len(dataframes)} tablas con eventos futuros")
                        self.save_tables_to_json(future_tables, artist_name)
                    else:
                        logger.warning(f"⚠️ NO se creará JSON para '{artist_name}': todos los eventos son pasados")
                        print(f"⚠️ NO SE CREA JSON: Todos los eventos de '{artist_name}' son pasados")
            else:
                logger.warning("⚠️ No se pudo procesar ninguna tabla")
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo tablas: {str(e)}")
    
    def extract_artist_name(self, full_option_text):
        """
        Extrae solo el nombre del artista del texto completo de la opción
        
        Args:
            full_option_text (str): Texto completo de la opción (ej: "DUKI_SHOW_EN_VELEZ")
            
        Returns:
            str: Nombre del artista extraído (ej: "DUKI")
        """
        try:
            logger.info(f"🔍 Extrayendo nombre del artista de: '{full_option_text}'")
            
            # Si el texto contiene guiones bajos, tomar la primera parte
            if '_' in full_option_text:
                artist_name = full_option_text.split('_')[0]
                logger.info(f"✅ Nombre del artista extraído: '{artist_name}'")
                return artist_name
            
            # Si el texto contiene espacios, usar el nombre completo (no solo la primera palabra)
            elif ' ' in full_option_text:
                # Usar todo el texto como nombre del artista
                artist_name = full_option_text
                logger.info(f"✅ Nombre del artista extraído: '{artist_name}'")
                return artist_name
            
            # Si no hay separadores, usar todo el texto
            else:
                logger.info(f"✅ Usando texto completo como nombre del artista: '{full_option_text}'")
                return full_option_text
                
        except Exception as e:
            logger.error(f"❌ Error extrayendo nombre del artista: {str(e)}")
            return full_option_text  # Fallback al texto original
    
    def group_options_by_artist(self, all_options):
        """
        Agrupa las opciones por nombre de artista
        
        Args:
            all_options (list): Lista de todas las opciones disponibles
            
        Returns:
            dict: Diccionario con artistas como claves y listas de opciones como valores
        """
        try:
            logger.info("🔍 Agrupando opciones por artista...")
            
            artist_groups = {}
            
            for option in all_options:
                # Extraer el nombre base del artista
                artist_name = self.extract_artist_name(option)
                
                # Normalizar el nombre del artista (quitar números y caracteres especiales)
                clean_artist_name = re.sub(r'\d+', '', artist_name).strip()
                clean_artist_name = re.sub(r'[^\w\s]', '', clean_artist_name).strip()
                
                # Si el nombre está vacío después de limpiar, usar el original
                if not clean_artist_name:
                    clean_artist_name = artist_name
                
                # Agregar la opción al grupo del artista
                if clean_artist_name not in artist_groups:
                    artist_groups[clean_artist_name] = []
                
                artist_groups[clean_artist_name].append(option)
                logger.info(f"📋 Agregando '{option}' al grupo '{clean_artist_name}'")
            
            # Mostrar resumen de grupos
            logger.info(f"📊 RESUMEN DE GRUPOS POR ARTISTA:")
            for artist, options in artist_groups.items():
                logger.info(f"  🎤 {artist}: {len(options)} opciones")
                for i, option in enumerate(options, 1):
                    logger.info(f"    {i}. {option}")
            
            return artist_groups
            
        except Exception as e:
            logger.error(f"❌ Error agrupando opciones por artista: {str(e)}")
            return {}
    
    def calculate_totals_from_dataframe(self, df):
        """
        Calcula los totales de capacidad, vendido, hold y disponible de un DataFrame
        
        Args:
            df: DataFrame con los datos de la tabla
            
        Returns:
            dict: Diccionario con los totales calculados
        """
        try:
            logger.info("🧮 Calculando totales de la tabla...")
            
            # Inicializar totales
            totals = {
                "total_capacidad": 0,
                "total_vendido": 0,
                "total_hold": 0,
                "total_disponible": 0,
                "total_pendiente_verificar": 0,
                "total_invitaciones": 0,
                "total_en_curso": 0
            }
            
            # Mapear nombres de columnas (por si varían)
            column_mapping = {
                "Capacidad": "total_capacidad",
                "Vendido": "total_vendido", 
                "Hold": "total_hold",
                "Disponible": "total_disponible",
                "Pendiente verificar": "total_pendiente_verificar",
                "Invitaciones": "total_invitaciones",
                "En curso": "total_en_curso"
            }
            
            # Calcular totales por cada fila
            for index, row in df.iterrows():
                for col_name, total_key in column_mapping.items():
                    if col_name in df.columns:
                        try:
                            # Extraer número del valor (remover caracteres no numéricos excepto puntos y comas)
                            value_str = str(row[col_name]).strip()
                            # Remover caracteres no numéricos excepto puntos y comas
                            numeric_str = re.sub(r'[^\d.,]', '', value_str)
                            
                            if numeric_str:
                                # Convertir a float (manejar tanto . como , como separador decimal)
                                if ',' in numeric_str and '.' in numeric_str:
                                    # Formato: 1,234.56 -> usar punto como decimal
                                    numeric_str = numeric_str.replace(',', '')
                                elif ',' in numeric_str:
                                    # Formato: 1234,56 -> usar coma como decimal
                                    numeric_str = numeric_str.replace(',', '.')
                                
                                value = float(numeric_str)
                                totals[total_key] += value
                                
                        except (ValueError, TypeError) as e:
                            logger.debug(f"⚠️ No se pudo convertir '{row[col_name]}' a número en columna '{col_name}': {str(e)}")
                            continue
            
            # Convertir totales a enteros (ya que son cantidades de tickets)
            for key in totals:
                totals[key] = int(totals[key])
            
            logger.info(f"✅ Totales calculados:")
            logger.info(f"  📊 Capacidad total: {totals['total_capacidad']}")
            logger.info(f"  🎫 Vendido total: {totals['total_vendido']}")
            logger.info(f"  🔒 Hold total: {totals['total_hold']}")
            logger.info(f"  🆓 Disponible total: {totals['total_disponible']}")
            logger.info(f"  ⏳ Pendiente verificar total: {totals['total_pendiente_verificar']}")
            logger.info(f"  🎁 Invitaciones total: {totals['total_invitaciones']}")
            logger.info(f"  🔄 En curso total: {totals['total_en_curso']}")
            
            return totals
            
        except Exception as e:
            logger.error(f"❌ Error calculando totales: {str(e)}")
            return {
                "total_capacidad": 0,
                "total_vendido": 0,
                "total_hold": 0,
                "total_disponible": 0,
                "total_pendiente_verificar": 0,
                "total_invitaciones": 0,
                "total_en_curso": 0
            }

    def get_tables_as_json_data(self, dataframes, artist_name):
        """
        Obtiene las tablas extraídas como datos JSON en memoria (sin archivos físicos)
        
        Args:
            dataframes (list): Lista de diccionarios con información de las tablas
            artist_name (str): Nombre del artista para identificar los datos
            
        Returns:
            dict: Datos de las tablas en formato JSON (en memoria)
        """
        try:
            logger.info(f"💾 Preparando datos JSON en memoria para artista: '{artist_name}'...")
            
            # Extraer solo el nombre del artista del texto completo
            clean_artist_name = self.extract_artist_name(artist_name)
            logger.info(f"🎯 Nombre del artista limpio: '{clean_artist_name}'")
            
            # Preparar datos para JSON (en memoria)
            # Restar 3 horas directamente para hora de Argentina (UTC-3)
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            
            json_data = {
                "artista": clean_artist_name,
                "fecha_extraccion": fecha_extraccion_utc3.isoformat(),
                "total_tablas": len(dataframes),
                "url": self.driver.current_url,
                "tablas": []
            }
            
            # Convertir DataFrames a formato JSON y calcular totales
            for table_info in dataframes:
                df = table_info['dataframe']
                
                # Calcular totales para esta tabla
                table_totals = self.calculate_totals_from_dataframe(df)
                
                # Convertir DataFrame a diccionario
                table_dict = {
                    "tabla_numero": table_info['tabla_numero'],
                    "fecha_evento": table_info.get('fecha_evento', 'Fecha no disponible'),
                    "filas": table_info['filas'],
                    "columnas": table_info['columnas'],
                    "nombres_columnas": list(df.columns),
                    "datos": df.to_dict('records'),  # Cada fila como un diccionario
                    "totales_tabla": table_totals  # Agregar totales de esta tabla
                }
                
                json_data["tablas"].append(table_dict)
            
            
            logger.info(f"✅ Datos JSON preparados en memoria para artista: '{clean_artist_name}'")
            logger.info(f"📊 Total de tablas: {len(dataframes)}")
            
            # Agregar a los datos finales del scraper
            self.final_data["datos_por_artista"][clean_artist_name] = json_data
            
            # Guardar en la base de datos
            if self.db_connected:
                self.save_to_database(clean_artist_name, json_data)
            
            return json_data
            
        except Exception as e:
            logger.error(f"❌ Error preparando datos JSON: {str(e)}")
            return None
    
    def create_or_get_show_ventas_funcion(self, artista, fecha_show):
        """
        Crea un show si no existe o devuelve el ID del show existente para ventas por función
        
        Args:
            artista (str): Nombre del artista
            fecha_show (str): Fecha del show en formato "15/10/2025 09:00:00 PM"
            
        Returns:
            str: ID del show
        """
        try:
            from datetime import datetime
            import pytz
            
            # Parsear la fecha del show - manejar ambos formatos Y convertir timezone
            try:
                # Primero intentar con segundos: 15/10/2025 09:00:00 PM
                fecha_parsed = datetime.strptime(fecha_show, "%d/%m/%Y %I:%M:%S %p")
            except ValueError:
                # Si falla, intentar sin segundos: 15/10/2025 09:00 PM
                fecha_parsed = datetime.strptime(fecha_show, "%d/%m/%Y %I:%M %p")
            
            # Convertir directamente sin timezone para que coincida con BD
            # BD tiene: 2025-09-16 21:00:00+00 (UTC)
            # Scraper: 16/09/2025 09:00:00 PM (local)
            # 09:00 PM = 21:00 en formato 24h
            fecha_show_utc = fecha_parsed
            
            # Obtener conexión
            connection = get_database_connection()
            if not connection:
                logger.error("❌ No se pudo obtener conexión a la base de datos")
                return None
                
            cursor = connection.cursor()
            
            # Buscar show existente - comparar fecha y hora con tolerancia
            cursor.execute("""
                SELECT id FROM shows 
                WHERE artista = %s 
                  AND DATE(fecha_show) = DATE(%s)
                  AND EXTRACT(hour FROM fecha_show) = EXTRACT(hour FROM %s)
                  AND venue = 'Movistar Arena'
                  AND (ticketera = 'movistar' OR ticketera = 'Movistar Arena')
            """, (artista, fecha_show_utc, fecha_show_utc))
            
            existing_show = cursor.fetchone()
            
            if existing_show:
                logger.info(f"✅ Show existente encontrado: {existing_show[0]}")
                cursor.close()
                connection.close()
                return existing_show[0]
            
            # Crear nuevo show
            import uuid
            show_id = str(uuid.uuid4())
            
            # Capacidad por defecto para Movistar Arena (se puede ajustar después)
            # CONDICIÓN ESPECIAL: DUKI tiene capacidad de 13,550
            if "DUKI" in artista.upper():
                capacidad_default = 13550  # Capacidad especial para DUKI
                logger.info(f"🎯 DUKI DETECTADO: Usando capacidad especial de {capacidad_default:,}")
            else:
                capacidad_default = 10718  # Capacidad estándar de Movistar Arena
            
            cursor.execute("""
                INSERT INTO shows (
                    id, artista, venue, fecha_show, ciudad, pais, 
                    ticketera, estado, capacidad_total, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                show_id,
                artista,
                "Movistar Arena",
                fecha_show_utc,
                "Buenos Aires",
                "Argentina",
                "movistar",
                "confirmado",
                capacidad_default,
                datetime.now(timezone.utc)
            ))
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info(f"✅ Nuevo show creado: {show_id} - {artista} - {fecha_show}")
            return show_id
            
        except Exception as e:
            logger.error(f"❌ Error creando/obteniendo show: {e}")
            return None

    def update_daily_sales_for_show_ventas_funcion(self, show_id, daily_breakdown_data, artista, fecha_show):
        """
        Actualiza todas las daily_sales de un show con los nuevos datos del desglose diario
        
        Args:
            show_id (str): ID del show
            daily_breakdown_data (list): Datos del desglose diario
            artista (str): Nombre del artista
            fecha_show (str): Fecha del show
        """
        try:
            # Obtener conexión
            connection = get_database_connection()
            if not connection:
                logger.error("❌ No se pudo obtener conexión a la base de datos")
                return False
                
            cursor = connection.cursor()
            
            logger.info(f"🔄 Actualizando daily_sales para show {artista} - {fecha_show}")
            
            # Procesar cada día del desglose
            registros_actualizados = 0
            registros_nuevos = 0
            
            for day_data in daily_breakdown_data:
                fecha_venta = day_data['fecha_venta']  # "25/02/2025"
                venta_diaria_raw = day_data['cantidad_vendida']  # "454"
                monto_diario_raw = day_data['monto_vendido']  # "45.640.000"
                
                # Limpiar la cantidad vendida
                if isinstance(venta_diaria_raw, str):
                    venta_diaria = int(venta_diaria_raw.replace('.', '').replace(',', ''))
                else:
                    venta_diaria = int(venta_diaria_raw or 0)
                
                # Limpiar el monto (quitar puntos y convertir a entero)
                if isinstance(monto_diario_raw, str):
                    monto_diario = int(monto_diario_raw.replace('.', '').replace(',', ''))
                else:
                    monto_diario = int(monto_diario_raw or 0)
                
                # Convertir fecha a formato datetime
                fecha_venta_dt = datetime.strptime(fecha_venta, "%d/%m/%Y").date()
                
                # Verificar si ya existe este registro
                cursor.execute("""
                    SELECT id FROM daily_sales 
                    WHERE show_id = %s AND fecha_venta = %s
                """, (show_id, fecha_venta_dt))
                
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # Actualizar registro existente
                    cursor.execute("""
                        UPDATE daily_sales SET
                            venta_diaria = %s,
                            monto_diario_ars = %s,
                            fecha_extraccion = %s,
                            ticketera = %s,
                            venta_diaria_definitiva = %s,
                            monto_diario_ars_definitivo = %s
                        WHERE id = %s
                    """, (
                        venta_diaria,
                        monto_diario,
                        datetime.now(timezone.utc),
                        "movistar",
                        venta_diaria,  # Marcamos como definitiva
                        monto_diario,  # Marcamos como definitiva
                        existing_record[0]
                    ))
                    registros_actualizados += 1
                    logger.info(f"🔄 Actualizado: {fecha_venta} - {venta_diaria} tickets - ${monto_diario:,}")
                    
                else:
                    # Crear nuevo registro
                    import uuid
                    daily_sale_id = str(uuid.uuid4())
                    
                    cursor.execute("""
                        INSERT INTO daily_sales (
                            id, show_id, fecha_venta, fecha_extraccion,
                            venta_diaria, monto_diario_ars, ticketera,
                            venta_diaria_definitiva, monto_diario_ars_definitivo
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        daily_sale_id,
                        show_id,
                        fecha_venta_dt,
                        datetime.now(timezone.utc),
                        venta_diaria,
                        monto_diario,
                        "movistar",
                        venta_diaria,  # Marcamos como definitiva
                        monto_diario   # Marcamos como definitiva
                    ))
                    registros_nuevos += 1
                    logger.info(f"➕ Nuevo registro: {fecha_venta} - {venta_diaria} tickets - ${monto_diario:,}")
            
            logger.info(f"📊 Resumen: {registros_actualizados} actualizados, {registros_nuevos} nuevos")
            
            # Calcular y actualizar totales acumulados
            self.calculate_and_update_accumulated_totals_ventas_funcion(cursor, show_id)
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info(f"✅ Daily sales actualizadas para {artista} - {len(daily_breakdown_data)} registros")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error actualizando daily_sales: {e}")
            return False

    def calculate_and_update_accumulated_totals_ventas_funcion(self, cursor, show_id):
        """
        Calcula y actualiza los totales acumulados día a día para todas las daily_sales de un show
        Incluye cálculo de tickets disponibles y porcentaje de ocupación
        """
        try:
            logger.info("📊 Calculando totales acumulados día a día...")
            
            # Obtener la capacidad total del show
            cursor.execute("""
                SELECT capacidad_total FROM shows WHERE id = %s
            """, (show_id,))
            
            capacidad_result = cursor.fetchone()
            if not capacidad_result:
                logger.error("❌ No se encontró la capacidad del show")
                return
                
            capacidad_total = capacidad_result[0]
            logger.info(f"🏟️ Capacidad total del show: {capacidad_total:,}")
            
            # Obtener todos los registros ordenados por fecha (cronológicamente)
            cursor.execute("""
                SELECT id, fecha_venta, venta_diaria, monto_diario_ars
                FROM daily_sales 
                WHERE show_id = %s
                ORDER BY fecha_venta ASC
            """, (show_id,))
            
            records = cursor.fetchall()
            logger.info(f"📋 Procesando {len(records)} días para calcular acumulados")
            
            venta_acumulada = 0
            recaudacion_acumulada = 0
            
            for i, record in enumerate(records, 1):
                record_id, fecha_venta, venta_diaria, monto_diario = record
                
                # Sumar las ventas y recaudación del día actual al acumulado
                venta_acumulada += venta_diaria or 0
                recaudacion_acumulada += monto_diario or 0
                
                # Calcular tickets disponibles y porcentaje de ocupación
                tickets_disponibles = capacidad_total - venta_acumulada
                porcentaje_ocupacion = (venta_acumulada / capacidad_total * 100) if capacidad_total > 0 else 0
                
                # Actualizar los totales acumulados para este día
                cursor.execute("""
                    UPDATE daily_sales SET
                        venta_total_acumulada = %s,
                        recaudacion_total_ars = %s,
                        tickets_disponibles = %s,
                        porcentaje_ocupacion = %s
                    WHERE id = %s
                """, (venta_acumulada, recaudacion_acumulada, tickets_disponibles, round(porcentaje_ocupacion, 2), record_id))
                
                # Log cada 50 días para no saturar
                if i % 50 == 0 or i == len(records):
                    logger.info(f"📊 Día {i}: {fecha_venta} → Acumulado: {venta_acumulada:,} tickets, ${recaudacion_acumulada:,}, Disponibles: {tickets_disponibles:,}, Ocupación: {porcentaje_ocupacion:.2f}%")
            
            logger.info(f"✅ Totales finales acumulados: {venta_acumulada:,} tickets, ${recaudacion_acumulada:,}")
            logger.info(f"🎯 Tickets disponibles finales: {capacidad_total - venta_acumulada:,}")
            logger.info(f"📊 Ocupación final: {(venta_acumulada / capacidad_total * 100):.2f}%")
                
        except Exception as e:
            logger.error(f"❌ Error calculando totales acumulados: {e}")

    def save_ventas_funcion_to_database(self, data_list, artist_name_real):
        """
        Guarda los datos de ventas por función en la base de datos y actualiza daily_sales
        
        Args:
            data_list (list): Lista de datos extraídos con desglose diario
        """
        try:
            if not data_list or len(data_list) == 0:
                logger.warning("⚠️ No hay datos para guardar en la base de datos")
                return False

            # Obtener conexión
            connection = get_database_connection()
            if not connection:
                logger.error("❌ No se pudo obtener conexión a la base de datos")
                return False
                
            cursor = connection.cursor()
            
            for item in data_list:
                # Guardar en raw_data
                cursor.execute("""
                    INSERT INTO raw_data (ticketera, json_data, fecha_extraccion)
                    VALUES (%s, %s, %s)
                """, (
                    'Movistar Arena',
                    json.dumps(item, ensure_ascii=False),
                    datetime.now(timezone.utc)
                ))
                
                # Procesar cada show individual
                if 'desglose_diario' in item and item['desglose_diario']:
                    # Usar el artista real que se pasó como parámetro
                    artista = artist_name_real
                    fecha_show = ''
                    
                    # Intentar obtener fecha del evento
                    fecha_show = item.get('fecha_evento', '')
                    
                    # Si no tiene fecha_evento, intentar con datos_ventas_funcion
                    if not fecha_show and 'datos_ventas_funcion' in item and item['datos_ventas_funcion']:
                        fecha_show = item['datos_ventas_funcion'][0].get('fecha_funcion', '')
                    
                    logger.info(f"🎤 Procesando: Artista='{artista}' (real), Fecha='{fecha_show}'")
                    
                    # Crear o obtener show
                    show_id = self.create_or_get_show_ventas_funcion(artista, fecha_show)
                    
                    if show_id:
                        # Actualizar daily_sales
                        success = self.update_daily_sales_for_show_ventas_funcion(
                            show_id, 
                            item['desglose_diario'], 
                            artista, 
                            fecha_show
                        )
                        
                        if success:
                            logger.info(f"✅ Show procesado exitosamente: {artista} - {fecha_show}")
                        else:
                            logger.error(f"❌ Error procesando show: {artista} - {fecha_show}")
            
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info(f"✅ {len(data_list)} registros guardados y procesados completamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando en base de datos: {e}")
            # Rollback si hay error
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'connection' in locals() and connection:
                    connection.rollback()
                    connection.close()
            except:
                pass
            return False

    def save_to_database(self, artist_name, json_data):
        """
        Guarda los datos del artista en la base de datos
        Y procesa automáticamente sectores y daily_sales
        
        Args:
            artist_name (str): Nombre del artista
            json_data (dict): Datos JSON del artista
        """
        try:
            if not self.db_connected:
                logger.warning("⚠️ Base de datos no conectada, no se pueden guardar datos")
                return False
            
            logger.info(f"💾 Guardando datos de '{artist_name}' en la base de datos...")
            
            # Obtener conexión
            connection = get_database_connection()
            if not connection:
                logger.error("❌ No se pudo obtener conexión a la base de datos")
                return False
            
            cursor = connection.cursor()
            
            # Preparar datos para inserción
            # Restar 3 horas directamente para hora de Argentina (UTC-3)
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            
            # DEBUG: Mostrar las fechas para verificar
            logger.info(f"🕐 Fecha UTC: {datetime.now()}")
            logger.info(f"🕐 Fecha Argentina (UTC-3): {fecha_extraccion_utc3}")
            logger.info(f"🕐 Fecha ISO Argentina: {fecha_extraccion_utc3.isoformat()}")
            
            # Procesar cada tabla (cada tabla representa un show diferente)
            tablas = json_data.get('tablas', [])
            if not tablas:
                logger.warning("⚠️ No hay tablas en el JSON para guardar")
                return False
            
            # Procesar cada tabla por separado
            for i, tabla in enumerate(tablas):
                fecha_evento = tabla.get('fecha_evento')
                if not fecha_evento:
                    logger.warning(f"⚠️ Tabla {i+1} no tiene fecha_evento, saltando...")
                    continue
                
                # Parsear fecha del evento
                fecha_evento_parsed = self.parse_fecha_evento(fecha_evento)
                if not fecha_evento_parsed:
                    continue
                
                # Calcular totales para este show específico
                totales_show = self.calculate_show_totals(tabla)
                
                # Crear JSON individual para este show con totales calculados
                json_individual = {
                    'artista': artist_name,
                    'venue': 'Movistar Arena',
                    'fecha_evento': fecha_evento,
                    'url': json_data.get('url', ''),
                    'totales_show': totales_show,  # Totales calculados
                    'tablas': [tabla]  # Solo esta tabla
                }
                
                # 1. GUARDAR EN RAW_DATA (como antes)
                raw_data_id = self.save_raw_data(cursor, artist_name, fecha_evento_parsed, json_individual, fecha_extraccion_utc3)
                
                # 2. CREAR/ACTUALIZAR SHOW
                show_id, vendido_anterior, recaudacion_anterior = self.create_or_update_show(cursor, artist_name, fecha_evento_parsed, totales_show)
                
                # 3. PROCESAR SECTORES
                self.process_sectores(cursor, show_id, tabla['datos'], fecha_extraccion_utc3)
                
                # 4. PROCESAR DAILY_SALES
                self.process_daily_sales(cursor, show_id, artist_name, fecha_evento_parsed, totales_show, fecha_extraccion_utc3, vendido_anterior, recaudacion_anterior)
                
                logger.info(f"✅ Procesamiento completo de '{artist_name}' - {fecha_evento} exitoso")
                print(f"✅ PROCESADO COMPLETO: {artist_name} - {fecha_evento}")
            
            # Commit y cerrar conexión
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info("✅ Todos los datos procesados exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos de '{artist_name}' en la BD: {str(e)}")
            print(f"❌ ERROR GUARDANDO EN BD: {artist_name} - {str(e)}")
            
            # Rollback si hay error
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'connection' in locals() and connection:
                    connection.rollback()
                    connection.close()
            except:
                pass
            
            return False
    
    def calculate_show_totals(self, tabla):
        """
        Calcula los totales de un show específico basándose en los datos de la tabla
        
        Args:
            tabla (dict): Datos de la tabla del show
            
        Returns:
            dict: Diccionario con todos los totales calculados
        """
        try:
            # Obtener totales de la tabla
            totales_tabla = tabla.get("totales_tabla", {})
            
            # Calcular totales del show
            totales_show = {
                "capacidad_total": totales_tabla.get("total_capacidad", 0),
                "vendido_total": totales_tabla.get("total_vendido", 0),
                "disponible_total": totales_tabla.get("total_disponible", 0),
                "hold_total": totales_tabla.get("total_hold", 0),
                "pendiente_verificar_total": totales_tabla.get("total_pendiente_verificar", 0),
                "invitaciones_total": totales_tabla.get("total_invitaciones", 0),
                "en_curso_total": totales_tabla.get("total_en_curso", 0),
                "recaudacion_total_ars": totales_tabla.get("total_recaudado", 0),
                "porcentaje_ocupacion": 0
            }
            
            # Calcular porcentaje de ocupación
            if totales_show["capacidad_total"] > 0:
                totales_show["porcentaje_ocupacion"] = round(
                    (totales_show["vendido_total"] / totales_show["capacidad_total"]) * 100, 2
                )
            
            logger.info(f"📊 Totales calculados para show:")
            logger.info(f"  📊 Capacidad: {totales_show['capacidad_total']}")
            logger.info(f"  🎫 Vendido: {totales_show['vendido_total']}")
            logger.info(f"  🆓 Disponible: {totales_show['disponible_total']}")
            logger.info(f"  💰 Recaudación: ${totales_show['recaudacion_total_ars']:,}")
            logger.info(f"  📈 Ocupación: {totales_show['porcentaje_ocupacion']}%")
            
            return totales_show
            
        except Exception as e:
            logger.error(f"❌ Error calculando totales del show: {str(e)}")
            return {
                "capacidad_total": 0,
                "vendido_total": 0,
                "disponible_total": 0,
                "hold_total": 0,
                "pendiente_verificar_total": 0,
                "invitaciones_total": 0,
                "en_curso_total": 0,
                "recaudacion_total_ars": 0,
                "porcentaje_ocupacion": 0
            }
    
    def parse_fecha_evento(self, fecha_evento):
        """
        Parsea la fecha del evento del formato original a formato de base de datos
        
        Args:
            fecha_evento (str): Fecha en formato "15/10/2025 09:00 PM"
            
        Returns:
            str: Fecha completa con hora en formato "2025-10-15 21:00:00" o None si hay error
        """
        try:
            from datetime import datetime
            
            # Formato: 15/10/2025 09:00 PM
            fecha_evento_parsed = datetime.strptime(fecha_evento, '%d/%m/%Y %I:%M %p')
            # Convertir a string en formato que coincida con la base de datos
            return fecha_evento_parsed.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.warning(f"⚠️ No se pudo parsear fecha del evento: {e}")
            return None
    
    def save_raw_data(self, cursor, artist_name, fecha_show, json_individual, fecha_extraccion):
        """
        Guarda datos en raw_data
        
        Args:
            cursor: Cursor de la base de datos
            artist_name (str): Nombre del artista
            fecha_show (str): Fecha del show
            json_individual (dict): JSON individual del show
            fecha_extraccion: Fecha de extracción
            
        Returns:
            str: ID del registro insertado
        """
        try:
            # Preparar datos para inserción
            insert_data = {
                "ticketera": "movistar",
                "artista": artist_name,
                "venue": "Movistar Arena",
                "fecha_show": fecha_show,
                "json_data": json.dumps(json_individual, ensure_ascii=False),
                "archivo_origen": f"movistar_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "url_origen": self.base_url,
                "fecha_extraccion": fecha_extraccion.isoformat(),
                "procesado": True  # Marcar como procesado inmediatamente
            }
            
            # Query de inserción
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
            cursor.execute(insert_query, insert_data)
            result = cursor.fetchone()
            
            if result:
                logger.info(f"✅ Raw data guardado: ID {result[0]}")
                return result[0]
            else:
                logger.warning("⚠️ Raw data insertado pero sin ID retornado")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error guardando raw data: {e}")
            raise e
    
    def create_or_update_show(self, cursor, artist_name, fecha_show, totales=None):
        """
        Busca show existente por artista y fecha, si no existe lo crea
        Si existe, actualiza los totales
        
        Args:
            cursor: Cursor de la base de datos
            artist_name (str): Nombre del artista
            fecha_show (str): Fecha del show
            totales (dict): Totales del show para actualizar
            
        Returns:
            tuple: (show_id, vendido_anterior, recaudacion_anterior)
        """
        try:
            # Buscar show existente con totales actuales
            cursor.execute("""
                SELECT id, capacidad_total
                FROM shows 
                WHERE artista = %s AND fecha_show = %s AND ticketera = 'movistar'
            """, (artist_name, fecha_show))
            
            result = cursor.fetchone()
            
            if result:
                # Show ya existe - obtener datos anteriores de daily_sales
                show_id = result[0]
                
                # Buscar último registro de daily_sales para obtener datos anteriores
                cursor.execute("""
                    SELECT venta_total_acumulada, recaudacion_total_ars
                    FROM daily_sales 
                    WHERE show_id = %s 
                    ORDER BY fecha_venta DESC, fecha_extraccion DESC 
                    LIMIT 1
                """, (show_id,))
                
                ultimo_daily_sales = cursor.fetchone()
                if ultimo_daily_sales:
                    vendido_anterior = ultimo_daily_sales[0] or 0
                    recaudacion_anterior = ultimo_daily_sales[1] or 0
                else:
                    vendido_anterior = 0
                    recaudacion_anterior = 0
                
                # Actualizar capacidad del show si es necesario
                if totales and totales.get('capacidad_total', 0) > 0:
                    cursor.execute("""
                        UPDATE shows SET 
                            capacidad_total = %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (totales.get('capacidad_total', 0), show_id))
                
                logger.info(f"✅ Show existente encontrado: {show_id}")
                return show_id, vendido_anterior, recaudacion_anterior
            else:
                # Crear nuevo show
                if totales:
                    # CONDICIÓN ESPECIAL: DUKI tiene capacidad de 13,550
                    capacidad_para_show = totales.get('capacidad_total', 0)
                    if "DUKI" in artist_name.upper() and capacidad_para_show == 0:
                        capacidad_para_show = 13550
                        logger.info(f"🎯 DUKI DETECTADO: Usando capacidad especial de {capacidad_para_show:,}")
                    
                    cursor.execute("""
                        INSERT INTO shows (artista, venue, fecha_show, ticketera, estado, capacidad_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        artist_name, 'Movistar Arena', fecha_show, 'movistar', 'activo',
                        capacidad_para_show
                    ))
                else:
                    # CONDICIÓN ESPECIAL: DUKI tiene capacidad de 13,550 incluso sin totales
                    if "DUKI" in artist_name.upper():
                        cursor.execute("""
                            INSERT INTO shows (artista, venue, fecha_show, ticketera, estado, capacidad_total)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            RETURNING id
                        """, (artist_name, 'Movistar Arena', fecha_show, 'movistar', 'activo', 13550))
                        logger.info(f"🎯 DUKI DETECTADO: Usando capacidad especial de 13,550")
                    else:
                        cursor.execute("""
                            INSERT INTO shows (artista, venue, fecha_show, ticketera, estado)
                            VALUES (%s, %s, %s, %s, %s)
                            RETURNING id
                        """, (artist_name, 'Movistar Arena', fecha_show, 'movistar', 'activo'))
                
                show_id = cursor.fetchone()[0]
                logger.info(f"✅ Nuevo show creado: {show_id}")
                return show_id, 0, 0  # Sin datos anteriores
            
        except Exception as e:
            logger.error(f"❌ Error creando/actualizando show: {e}")
            raise e
    
    def process_sectores(self, cursor, show_id, sectores_data, fecha_extraccion):
        """
        Procesa cada sector del JSON y lo guarda en la tabla sectores
        
        Args:
            cursor: Cursor de la base de datos
            show_id (str): ID del show
            sectores_data (list): Lista de sectores del JSON
            fecha_extraccion: Fecha de extracción
        """
        try:
            logger.info(f"🏟️ Procesando {len(sectores_data)} sectores...")
            
            for sector in sectores_data:
                # Limpiar datos del sector
                sector_limpio = self.clean_sector_data(sector)
                
                # Guardar en tabla sectores
                cursor.execute("""
                    INSERT INTO sectores (
                        show_id, ticketera, nombre_sector, capacidad, vendidas, 
                        disponibles, bloqueadas, invitaciones, precio, total_recaudado,
                        fecha_extraccion
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    show_id,
                    'Movistar Arena',
                    sector_limpio['nombre_sector'],
                    sector_limpio['capacidad'],
                    sector_limpio['vendidas'],
                    sector_limpio['disponibles'],
                    sector_limpio['bloqueadas'],
                    sector_limpio['invitaciones'],
                    sector_limpio['precio'],
                    sector_limpio['total_recaudado'],
                    fecha_extraccion
                ))
            
            logger.info(f"✅ {len(sectores_data)} sectores procesados exitosamente")
            
        except Exception as e:
            logger.error(f"❌ Error procesando sectores: {e}")
            raise e
    
    def process_daily_sales(self, cursor, show_id, artist_name, fecha_show, totales_show, fecha_extraccion, vendido_anterior=0, recaudacion_anterior=0):
        """
        Procesa daily_sales - UN SOLO registro por día
        
        Args:
            cursor: Cursor de la base de datos
            show_id (str): ID del show
            artist_name (str): Nombre del artista
            fecha_show (str): Fecha del show
            totales_show (dict): Totales del show
            fecha_extraccion: Fecha de extracción
            vendido_anterior (int): Tickets vendidos anteriormente
            recaudacion_anterior (int): Recaudación anterior
        """
        try:
            fecha_venta = fecha_extraccion.date()
            
            # Verificar si ya existe registro del día
            cursor.execute("""
                SELECT id, venta_total_acumulada, recaudacion_total_ars
                FROM daily_sales 
                WHERE show_id = %s AND fecha_venta = %s
            """, (show_id, fecha_venta))
            
            registro_existente = cursor.fetchone()
            
            if registro_existente:
                # ACTUALIZAR registro existente
                self.update_daily_sales_record(cursor, registro_existente[0], show_id, totales_show, fecha_extraccion, vendido_anterior, recaudacion_anterior)
            else:
                # CREAR nuevo registro
                self.create_daily_sales_record(cursor, show_id, totales_show, fecha_extraccion, vendido_anterior, recaudacion_anterior)
            
        except Exception as e:
            logger.error(f"❌ Error procesando daily_sales: {e}")
            raise e
    
    def create_daily_sales_record(self, cursor, show_id, totales_show, fecha_extraccion, vendido_anterior=0, recaudacion_anterior=0):
        """
        Crea nuevo registro en daily_sales
        
        Args:
            cursor: Cursor de la base de datos
            show_id (str): ID del show
            totales_show (dict): Totales del show
            fecha_extraccion: Fecha de extracción
            vendido_anterior (int): Tickets vendidos anteriormente
            recaudacion_anterior (int): Recaudación anterior
        """
        try:
            # Calcular venta diaria usando datos anteriores del show
            venta_diaria_total = totales_show['vendido_total'] - vendido_anterior
            recaudacion_diaria_total = totales_show['recaudacion_total_ars'] - recaudacion_anterior
            
            # Crear registro
            cursor.execute("""
                INSERT INTO daily_sales (
                    show_id, fecha_venta, fecha_extraccion, venta_diaria, monto_diario_ars,
                    venta_total_acumulada, recaudacion_total_ars, tickets_disponibles,
                    porcentaje_ocupacion, archivo_origen, ticketera
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                show_id,
                fecha_extraccion.date(),
                fecha_extraccion,
                venta_diaria_total,
                recaudacion_diaria_total,
                totales_show['vendido_total'],
                totales_show['recaudacion_total_ars'],
                totales_show['disponible_total'],
                totales_show['porcentaje_ocupacion'],
                f"movistar_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                'Movistar Arena'
            ))
            
            logger.info(f"✅ Nuevo registro daily_sales creado: {venta_diaria_total} tickets, ${recaudacion_diaria_total:,}")
            
        except Exception as e:
            logger.error(f"❌ Error creando registro daily_sales: {e}")
            raise e
    
    def update_daily_sales_record(self, cursor, daily_sales_id, show_id, totales_show, fecha_extraccion, vendido_anterior=0, recaudacion_anterior=0):
        """
        Actualiza registro existente en daily_sales
        
        Args:
            cursor: Cursor de la base de datos
            daily_sales_id (str): ID del registro daily_sales
            show_id (str): ID del show
            totales_show (dict): Totales del show
            fecha_extraccion: Fecha de extracción
            vendido_anterior (int): Tickets vendidos anteriormente
            recaudacion_anterior (int): Recaudación anterior
        """
        try:
            # Recalcular venta diaria usando datos anteriores del show
            venta_diaria_total = totales_show['vendido_total'] - vendido_anterior
            recaudacion_diaria_total = totales_show['recaudacion_total_ars'] - recaudacion_anterior
            
            # Actualizar registro
            cursor.execute("""
                UPDATE daily_sales SET
                    fecha_extraccion = %s,
                    venta_diaria = %s,
                    monto_diario_ars = %s,
                    venta_total_acumulada = %s,
                    recaudacion_total_ars = %s,
                    tickets_disponibles = %s,
                    porcentaje_ocupacion = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (
                fecha_extraccion,
                venta_diaria_total,
                recaudacion_diaria_total,
                totales_show['vendido_total'],
                totales_show['recaudacion_total_ars'],
                totales_show['disponible_total'],
                totales_show['porcentaje_ocupacion'],
                daily_sales_id
            ))
            
            logger.info(f"✅ Registro daily_sales actualizado: {venta_diaria_total} tickets, ${recaudacion_diaria_total:,}")
            
        except Exception as e:
            logger.error(f"❌ Error actualizando registro daily_sales: {e}")
            raise e
    
    def get_last_show_data_previous_day(self, cursor, show_id, fecha_actual):
        """
        Busca el último registro del mismo show del día anterior
        
        Args:
            cursor: Cursor de la base de datos
            show_id (str): ID del show
            fecha_actual: Fecha actual
            
        Returns:
            dict: Datos del último registro del día anterior o None
        """
        try:
            cursor.execute("""
                SELECT venta_total_acumulada, recaudacion_total_ars
                FROM daily_sales 
                WHERE show_id = %s 
                AND fecha_venta = %s - INTERVAL '1 day'
                ORDER BY fecha_extraccion DESC
                LIMIT 1
            """, (show_id, fecha_actual))
            
            result = cursor.fetchone()
            
            if result:
                return {
                    'vendido_total': result[0],
                    'recaudacion_total': result[1]
                }
            else:
                return None
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo último registro día anterior: {e}")
            return None
    
    def clean_sector_data(self, sector):
        """
        Limpia y convierte datos del sector
        
        Args:
            sector (dict): Datos del sector del JSON
            
        Returns:
            dict: Datos limpios del sector
        """
        try:
            return {
                'nombre_sector': sector.get('Sector', ''),
                'capacidad': int(sector.get('Capacidad', 0)),
                'vendidas': int(sector.get('Vendido', 0)),
                'disponibles': int(sector.get('Disponible', 0)),
                'bloqueadas': int(sector.get('Hold', 0)),
                'invitaciones': int(sector.get('Invitaciones', 0)),
                'precio': self.parse_precio(sector.get('Precio', '0')),
                'total_recaudado': self.parse_recaudado(sector.get('Recaudado', '0'))
            }
        except Exception as e:
            logger.error(f"❌ Error limpiando datos del sector: {e}")
            return {
                'nombre_sector': '',
                'capacidad': 0,
                'vendidas': 0,
                'disponibles': 0,
                'bloqueadas': 0,
                'invitaciones': 0,
                'precio': 0,
                'total_recaudado': 0
            }
    
    def parse_precio(self, precio_str):
        """
        Convierte string de precio a número
        
        Args:
            precio_str (str): String del precio (ej: "$ 80.000")
            
        Returns:
            int: Precio como número
        """
        try:
            # Remover $ y espacios, convertir a int
            precio_limpio = precio_str.replace('$', '').replace('.', '').replace(',', '').replace(' ', '').strip()
            return int(precio_limpio) if precio_limpio.isdigit() else 0
        except:
            return 0
    
    def parse_recaudado(self, recaudado_str):
        """
        Convierte string de recaudado a número
        
        Args:
            recaudado_str (str): String del recaudado (ej: "$ 16.640.000")
            
        Returns:
            int: Recaudado como número
        """
        try:
            # Remover $ y espacios, convertir a int
            recaudado_limpio = recaudado_str.replace('$', '').replace('.', '').replace(',', '').replace(' ', '').strip()
            return int(recaudado_limpio) if recaudado_limpio.isdigit() else 0
        except:
            return 0
    
    def get_first_input_options(self):
        """
        Obtiene todas las opciones disponibles del primer input
        
        Returns:
            list: Lista de textos de las opciones disponibles
        """
        try:
            logger.info("🔍 Obteniendo todas las opciones del primer input...")
            
            # Buscar divs con clase específica
            divs = self.driver.find_elements(By.CSS_SELECTOR, "div.mud-select.mud-autocomplete")
            if not divs:
                logger.error("❌ No se encontraron divs con clase 'mud-select mud-autocomplete'")
                return []
            
            first_div = divs[0]
            
            # Buscar el input dentro del div para hacer clic
            try:
                input_element = first_div.find_element(By.CSS_SELECTOR, "input.mud-input-slot")
                logger.info("✅ Input encontrado dentro del div")
                
                # Hacer scroll al elemento
                self.driver.execute_script("arguments[0].scrollIntoView(true);", input_element)
                time.sleep(0.5)
                
                # Hacer clic en el input para abrir el dropdown
                self.driver.execute_script("arguments[0].click();", input_element)
                logger.info("✅ Input clickeado para obtener opciones...")
                time.sleep(3)
                
            except Exception as e:
                logger.warning(f"⚠️ No se encontró input específico, usando div: {str(e)}")
                self.driver.execute_script("arguments[0].click();", first_div)
                logger.info("✅ Div clickeado como fallback...")
                time.sleep(2)
            
            # Buscar todas las opciones disponibles
            options_texts = []
            selectors = [
                ".mud-list-item",
                "li",
                ".mud-menu-item",
                ".mud-autocomplete-option",
                "[role='option']",
                ".mud-list .mud-list-item",
                ".mud-popover .mud-list-item",
                ".mud-popover li"
            ]
            
            for selector in selectors:
                try:
                    found_options = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    visible_options = [opt for opt in found_options if opt.is_displayed()]
                    if visible_options:
                        logger.info(f"✅ Encontradas {len(visible_options)} opciones visibles con selector: {selector}")
                        
                        for option in visible_options:
                            try:
                                option_text = option.text.strip()
                                if option_text:
                                    options_texts.append(option_text)
                            except:
                                continue
                        break
                except Exception as e:
                    logger.debug(f"Selector {selector} no funcionó: {str(e)}")
                    continue
            
            # Filtrar opciones para excluir elementos de navegación
            filtered_options = []
            navigation_keywords = ['home', '/', 'productores', 'histórico', 'venta', 'diaria', 'reportes', 'inicio']
            
            for option_text in options_texts:
                # Convertir a minúsculas para comparación
                option_lower = option_text.lower().strip()
                
                # Saltar si es un elemento de navegación
                is_navigation = any(keyword in option_lower for keyword in navigation_keywords)
                
                if not is_navigation and len(option_text.strip()) > 2:
                    filtered_options.append(option_text)
                    logger.info(f"✅ Opción válida: '{option_text}'")
                else:
                    logger.info(f"⏭️ Opción filtrada (navegación): '{option_text}'")
            
            logger.info(f"📋 Total de opciones encontradas: {len(options_texts)}")
            logger.info(f"📋 Opciones válidas después del filtro: {len(filtered_options)}")
            for i, option_text in enumerate(filtered_options):
                logger.info(f"  📋 Opción válida {i+1}: '{option_text}'")
            
            # Cerrar el dropdown haciendo clic fuera
            try:
                self.driver.execute_script("document.body.click();")
                time.sleep(1)
            except:
                pass
            
            return filtered_options
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo opciones del primer input: {str(e)}")
            return []

    def clear_and_reset_first_input(self):
        """
        Limpia y resetea el primer input para poder seleccionar otra opción
        
        Returns:
            bool: True si el reseteo fue exitoso, False en caso contrario
        """
        try:
            logger.info("🔄 Limpiando y reseteando el primer input...")
            
            # Buscar divs con clase específica
            divs = self.driver.find_elements(By.CSS_SELECTOR, "div.mud-select.mud-autocomplete")
            if not divs:
                logger.error("❌ No se encontraron divs con clase 'mud-select mud-autocomplete'")
                return False
            
            first_div = divs[0]
            
            try:
                input_element = first_div.find_element(By.CSS_SELECTOR, "input.mud-input-slot")
                logger.info("✅ Input encontrado para reseteo")
                
                # Hacer scroll al elemento
                self.driver.execute_script("arguments[0].scrollIntoView(true);", input_element)
                time.sleep(0.5)
                
                # Hacer clic en el input
                self.driver.execute_script("arguments[0].click();", input_element)
                time.sleep(1)
                
                # Seleccionar todo el texto y borrarlo
                self.driver.execute_script("arguments[0].select();", input_element)
                time.sleep(0.5)
                input_element.send_keys(Keys.DELETE)
                time.sleep(0.5)
                
                # También intentar con clear()
                input_element.clear()
                time.sleep(0.5)
                
                # Hacer clic fuera para cerrar cualquier dropdown
                self.driver.execute_script("document.body.click();")
                time.sleep(1)
                
                logger.info("✅ Input limpiado y reseteado exitosamente")
                return True
                                
            except Exception as e:
                logger.warning(f"⚠️ Error reseteando input específico: {str(e)}")
                # Intentar refrescar la página como último recurso
                try:
                    logger.info("🔄 Intentando refrescar la página...")
                    self.driver.refresh()
                    time.sleep(5)
                    logger.info("✅ Página refrescada")
                    return True
                except Exception as e2:
                    logger.error(f"❌ Error refrescando la página: {str(e2)}")
                    return False
                
        except Exception as e:
            logger.error(f"❌ Error en reseteo del input: {str(e)}")
            return False

    def try_manual_typing(self, option_text, option_index, artist_options):
        """
        Intenta escribir manualmente la opción en el input con reintentos
        
        Args:
            option_text (str): Texto a escribir
            option_index (int): Índice de la opción en la lista del artista
            artist_options (list): Lista completa de opciones del artista
            
        Returns:
            bool: True si la escritura fue exitosa, False en caso contrario
        """
        max_retries = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"⌨️ Intento {attempt + 1}/{max_retries} - Escribiendo manualmente: '{option_text}'")
                
                # Buscar divs con clase específica
                divs = self.driver.find_elements(By.CSS_SELECTOR, "div.mud-select.mud-autocomplete")
                if not divs:
                    logger.error("❌ No se encontraron divs con clase 'mud-select mud-autocomplete'")
                    if attempt < max_retries - 1:
                        logger.info("🔄 Recargando página para reintentar...")
                        self.driver.refresh()
                        time.sleep(5)
                        # Esperar a que la página cargue completamente
                        WebDriverWait(self.driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                        )
                        logger.info("✅ Página cargada completamente después de recarga")
                        continue
                    return False
                
                first_div = divs[0]
                
                try:
                    input_element = first_div.find_element(By.CSS_SELECTOR, "input.mud-input-slot")
                    logger.info("✅ Input encontrado para escritura manual")
                    
                    # Hacer scroll al elemento
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", input_element)
                    time.sleep(0.5)
                    
                    # Hacer clic en el input
                    self.driver.execute_script("arguments[0].click();", input_element)
                    time.sleep(1)
                    
                    # Limpiar el input
                    input_element.clear()
                    time.sleep(0.5)
                    
                    # Escribir el texto carácter por carácter
                    for char in option_text:
                        input_element.send_keys(char)
                        time.sleep(0.1)  # Pausa entre caracteres
                    
                    logger.info(f"✅ Texto '{option_text}' escrito manualmente")
                    time.sleep(2)  # Esperar a que aparezcan las opciones
                    
                    # Buscar la opción específica que queremos
                    try:
                        options = self.driver.find_elements(By.CSS_SELECTOR, ".mud-list-item")
                        visible_options = [opt for opt in options if opt.is_displayed()]
                        
                        if visible_options:
                            # Buscar la opción exacta que queremos
                            target_option = None
                            matching_options = []
                            
                            # Primero, encontrar todas las opciones que coinciden exactamente
                            for option in visible_options:
                                option_text_found = option.text.strip()
                                if option_text_found == option_text:
                                    matching_options.append(option)
                            
                            # Si encontramos opciones que coinciden exactamente
                            if matching_options:
                                # Calcular cuántas veces hemos visto esta opción antes
                                occurrences_before = 0
                                for i in range(option_index):
                                    if artist_options[i] == option_text:
                                        occurrences_before += 1
                                
                                # Seleccionar la opción correspondiente (primera, segunda, tercera, etc.)
                                # CONDICIÓN ESPECIAL: Para Diego Torres siempre seleccionar la segunda opción (índice 1)
                                if option_text == "Diego Torres" and len(matching_options) > 1:
                                    target_option = matching_options[1]  # Seleccionar siempre la segunda opción
                                    logger.info(f"🎯 CONDICIÓN ESPECIAL - Diego Torres: Encontradas {len(matching_options)} opciones, seleccionando SIEMPRE la #2")
                                elif occurrences_before < len(matching_options):
                                    target_option = matching_options[occurrences_before]
                                    logger.info(f"🎯 Encontradas {len(matching_options)} opciones exactas para '{option_text}', seleccionando la #{occurrences_before + 1}")
                                else:
                                    # Si no hay suficientes opciones, usar la última disponible
                                    target_option = matching_options[-1]
                                    logger.info(f"🎯 Encontradas {len(matching_options)} opciones exactas para '{option_text}', seleccionando la última disponible")
                            else:
                                # Si no hay coincidencias exactas, buscar opciones que contengan el texto
                                for option in visible_options:
                                    option_text_found = option.text.strip()
                                    if option_text in option_text_found:
                                        matching_options.append(option)
                                
                                if matching_options:
                                    # Calcular cuántas veces hemos visto esta opción antes
                                    occurrences_before = 0
                                    for i in range(option_index):
                                        if artist_options[i] == option_text:
                                            occurrences_before += 1
                                    
                                    # Seleccionar la opción correspondiente
                                    # CONDICIÓN ESPECIAL: Para Diego Torres siempre seleccionar la segunda opción (índice 1)
                                    if option_text == "Diego Torres" and len(matching_options) > 1:
                                        target_option = matching_options[1]  # Seleccionar siempre la segunda opción
                                        logger.info(f"🎯 CONDICIÓN ESPECIAL - Diego Torres: Encontradas {len(matching_options)} opciones que contienen texto, seleccionando SIEMPRE la #2")
                                    elif occurrences_before < len(matching_options):
                                        target_option = matching_options[occurrences_before]
                                        logger.info(f"🎯 Encontradas {len(matching_options)} opciones que contienen '{option_text}', seleccionando la #{occurrences_before + 1}")
                                    else:
                                        # Si no hay suficientes opciones, usar la última disponible
                                        target_option = matching_options[-1]
                                        logger.info(f"🎯 Encontradas {len(matching_options)} opciones que contienen '{option_text}', seleccionando la última disponible")
                            
                            if target_option:
                                logger.info(f"🎯 Haciendo clic en la opción específica: '{option_text}'")
                                self.driver.execute_script("arguments[0].click();", target_option)
                                logger.info("✅ Opción específica clickeada después de escritura manual")
                                time.sleep(2)
                                return True
                            else:
                                # Si no encontramos la opción exacta, usar la primera que contenga nuestro texto
                                for option in visible_options:
                                    option_text_found = option.text.strip()
                                    if option_text in option_text_found:
                                        target_option = option
                                        break
                                
                                if target_option:
                                    logger.info(f"🎯 Haciendo clic en opción que contiene el texto: '{option_text}'")
                                    self.driver.execute_script("arguments[0].click();", target_option)
                                    logger.info("✅ Opción que contiene el texto clickeada")
                                    time.sleep(2)
                                    return True
                                else:
                                    # Como último recurso, usar la primera opción
                                    first_option = visible_options[0]
                                    option_text_found = first_option.text.strip()
                                    logger.info(f"🎯 Haciendo clic en la primera opción como fallback: '{option_text_found}'")
                                    self.driver.execute_script("arguments[0].click();", first_option)
                                    logger.info("✅ Primera opción clickeada como fallback")
                                    time.sleep(2)
                                    return True
                        else:
                                                    logger.warning("⚠️ No se encontraron opciones después de la escritura manual")
                        if attempt < max_retries - 1:
                            logger.info("🔄 Recargando página para reintentar...")
                            self.driver.refresh()
                            time.sleep(5)
                            # Esperar a que la página cargue completamente
                            WebDriverWait(self.driver, 15).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                            )
                            logger.info("✅ Página cargada completamente después de recarga")
                            continue
                        return False
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Error clickeando opción después de escritura manual: {str(e)}")
                        if attempt < max_retries - 1:
                            logger.info("🔄 Recargando página para reintentar...")
                            self.driver.refresh()
                            time.sleep(5)
                            # Esperar a que la página cargue completamente
                            WebDriverWait(self.driver, 15).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                            )
                            logger.info("✅ Página cargada completamente después de recarga")
                            continue
                        return False
                
                except Exception as e:
                    logger.error(f"❌ Error en escritura manual: {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info("🔄 Recargando página para reintentar...")
                        self.driver.refresh()
                        time.sleep(5)
                        # Esperar a que la página cargue completamente
                        WebDriverWait(self.driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                        )
                        logger.info("✅ Página cargada completamente después de recarga")
                        continue
                    return False
                
            except Exception as e:
                logger.error(f"❌ Error en función de escritura manual (intento {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    logger.info("🔄 Recargando página para reintentar...")
                    self.driver.refresh()
                    time.sleep(5)
                    # Esperar a que la página cargue completamente
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                    )
                    logger.info("✅ Página cargada completamente después de recarga")
                    continue
                return False
        
        logger.error(f"❌ Falló la escritura manual después de {max_retries} intentos")
        return False
    
    def process_artist_group(self, artist_name, artist_options):
        """
        Procesa todas las opciones de un artista y acumula los datos
        
        Args:
            artist_name (str): Nombre del artista
            artist_options (list): Lista de opciones para este artista
            
        Returns:
            bool: True si el procesamiento fue exitoso, False en caso contrario
        """
        try:
            logger.info(f"🎤 PROCESANDO GRUPO DE ARTISTA: '{artist_name}'")
            logger.info(f"📋 Opciones a procesar: {len(artist_options)}")
            logger.info("=" * 60)
            
            # Lista para acumular todas las tablas de este artista
            all_artist_tables = []
            
            # Procesar cada opción del artista
            # CONDICIÓN ESPECIAL: Para Diego Torres, solo procesar la SEGUNDA opción para evitar duplicados
            options_to_process = artist_options
            if artist_name == "Diego Torres" and len(artist_options) > 1:
                options_to_process = [artist_options[1]]  # Solo la SEGUNDA opción (índice 1)
                logger.info(f"🎯 CONDICIÓN ESPECIAL - Diego Torres: Procesando solo la SEGUNDA de {len(artist_options)} opciones para evitar duplicados")
            
            for option_index, option_text in enumerate(options_to_process):
                try:
                    logger.info(f"🔄 Procesando opción {option_index + 1}/{len(options_to_process)}: '{option_text}'")
                    
                    # Procesar la opción individual y obtener las tablas
                    success, tables = self.process_single_option_for_group(option_text, option_index, artist_options)
                    
                    if success and tables:
                        all_artist_tables.extend(tables)
                        logger.info(f"✅ Opción '{option_text}' procesada exitosamente - {len(tables)} tablas agregadas")
                        print(f"✅ OPCIÓN {option_index + 1}: {option_text} - {len(tables)} tablas")
                    else:
                        logger.warning(f"⚠️ Opción '{option_text}' no produjo tablas válidas")
                        print(f"⚠️ OPCIÓN {option_index + 1}: {option_text} - Sin tablas")
                    
                    # Recargar la página después de cada opción (excepto la última)
                    if option_index < len(artist_options) - 1:
                        logger.info("🔄 Recargando página para la siguiente opción...")
                        self.driver.refresh()
                        time.sleep(5)
                        # Esperar a que la página cargue completamente
                        WebDriverWait(self.driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                        )
                        logger.info("✅ Página cargada completamente después de recarga")
                    # También recargar si no hay tablas válidas (para limpiar el input)
                    elif not tables:
                        logger.info("🔄 Recargando página porque no hay tablas válidas...")
                        self.driver.refresh()
                        time.sleep(5)
                        # Esperar a que la página cargue completamente
                        WebDriverWait(self.driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                        )
                        logger.info("✅ Página cargada completamente después de recarga")
                    
                    # Pausa entre opciones
                    if option_index < len(artist_options) - 1:
                        logger.info("⏳ Pausa de 3 segundos antes de la siguiente opción...")
                        time.sleep(3)
                        
                except Exception as e:
                    logger.error(f"❌ Error procesando opción '{option_text}': {str(e)}")
                    continue
            
            # Guardar datos de ventas por función con desglose diario en la base de datos
            if all_artist_tables:
                logger.info(f"💾 Guardando {len(all_artist_tables)} shows con desglose diario para '{artist_name}' en la base de datos")
                print(f"💾 GUARDANDO EN BD: {artist_name} - {len(all_artist_tables)} shows")
                
                # Usar la nueva función de guardado para ventas por función
                success = self.save_ventas_funcion_to_database(all_artist_tables, artist_name)
                
                if success:
                    logger.info(f"✅ Datos guardados exitosamente en BD para '{artist_name}'")
                    print(f"✅ GUARDADO EXITOSO: {artist_name} - {len(all_artist_tables)} shows")
                    
                    # Recargar la página después de procesar exitosamente
                    logger.info("🔄 Recargando página después de guardar datos exitosamente...")
                    self.driver.refresh()
                    time.sleep(5)
                    logger.info("✅ Página recargada después de guardar datos")
                    
                    return True
                else:
                    logger.error(f"❌ Error guardando datos en BD para '{artist_name}'")
                    return False
            else:
                logger.warning(f"⚠️ No se encontraron tablas válidas para '{artist_name}'")
                print(f"⚠️ SIN TABLAS: {artist_name}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error procesando grupo de artista '{artist_name}': {str(e)}")
            return False
    
    def process_single_option_for_group(self, option_text, option_index, artist_options):
        """
        Procesa una opción específica del primer input y retorna las tablas encontradas
        
        Args:
            option_text (str): Texto de la opción a procesar
            option_index (int): Índice de la opción (0-based)
            artist_options (list): Lista completa de opciones del artista
            
        Returns:
            tuple: (success, tables_list) donde success es bool y tables_list es lista de tablas
        """
        try:
            # Guardar el nombre del artista original para usar en reselecciones
            original_artist_name = option_text
            logger.info(f"🎯 PROCESANDO OPCIÓN {option_index + 1}: '{option_text}' (ESCRITURA MANUAL)")
            logger.info("=" * 60)
            
            # Usar siempre escritura manual para seleccionar el artista
            logger.info(f"⌨️ Escribiendo manualmente el artista: '{option_text}'")
            option_found = self.try_manual_typing(option_text, option_index, artist_options)
            
            if not option_found:
                logger.error(f"❌ No se pudo escribir manualmente la opción '{option_text}'")
                return False
            
            logger.info(f"✅ Opción '{option_text}' seleccionada exitosamente con escritura manual")
            
            # Continuar con el procesamiento del segundo input
            # Buscar input usando XPath específico
            logger.info("🔍 Buscando input de eventos con XPath específico...")
            evento_input = None
            
            try:
                evento_input = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "/html/body/div[3]/div/div/div[1]/div[4]/div/div/div/div[1]/input"))
                )
                logger.info("✅ Input de eventos encontrado con XPath específico")
            except Exception as e:
                logger.error(f"❌ Error encontrando input con XPath específico: {str(e)}")
                return False
            
            if not evento_input:
                logger.error("❌ No se pudo encontrar el input de eventos")
                return False
            
            # Hacer scroll al elemento y hacer clic
            self.driver.execute_script("arguments[0].scrollIntoView(true);", evento_input)
            time.sleep(1)
            self.driver.execute_script("arguments[0].click();", evento_input)
            logger.info("✅ Input de eventos clickeado")
            time.sleep(3)
            
            # Analizar fechas de las opciones antes de seleccionar
            logger.info("📅 Analizando fechas de las opciones...")
            
            selectors_to_try = [
                ".mud-list-item",
                "li",
                ".mud-menu-item",
                "[role='option']",
                ".mud-popover li",
                ".mud-select-item"
            ]
            
            all_options_found = []
            visible_options = []
            
            # Primero obtener todas las opciones
            for selector in selectors_to_try:
                try:
                    new_options = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    visible_options = [opt for opt in new_options if opt.is_displayed()]
                    
                    if visible_options:
                        logger.info(f"📋 Encontradas {len(visible_options)} opciones visibles con selector: {selector}")
                        
                        # Console log todas las opciones encontradas
                        print(f"\n=== OPCIONES ENCONTRADAS CON SELECTOR '{selector}' ===")
                        for i, option in enumerate(visible_options, 1):
                            try:
                                option_text_check = option.text.strip()
                                if option_text_check:
                                    all_options_found.append(option_text_check)
                                    print(f"{i}. {option_text_check}")
                                    logger.info(f"  📋 Opción {i}: '{option_text_check}'")
                            except Exception as e:
                                logger.debug(f"Error procesando opción {i}: {str(e)}")
                                continue
                        
                        print(f"Total de opciones: {len(all_options_found)}")
                        print("=" * 50)
                        break
                            
                except Exception as e:
                    logger.debug(f"Selector {selector} no funcionó: {str(e)}")
                    continue
            
            # Analizar fechas de las opciones (excluyendo "Seleccionar todos")
            event_options = []
            seleccionar_todos_option = None
            
            for i, option_text in enumerate(all_options_found):
                if "Seleccionar todos" in option_text:
                    seleccionar_todos_option = visible_options[i]
                    logger.info(f"🎯 Opción 'Seleccionar todos' encontrada en índice {i}")
                else:
                    event_options.append({
                        'index': i,
                        'text': option_text,
                        'element': visible_options[i]
                    })
            
            logger.info(f"📅 Analizando {len(event_options)} opciones de eventos...")
            
            # Obtener fecha actual en Buenos Aires
            buenos_aires_tz = pytz.timezone('America/Argentina/Buenos_Aires')
            now_ba = datetime.now(buenos_aires_tz)
            logger.info(f"🕐 Fecha actual en Buenos Aires: {now_ba.strftime('%d/%m/%Y %I:%M %p')}")
            
            # Analizar cada opción de evento
            future_events = []
            past_events = []
            
            for event_option in event_options:
                try:
                    option_text = event_option['text']
                    logger.info(f"🔍 Analizando opción: '{option_text}'")
                    
                    # Extraer fecha del formato 'show | dd/mm/aa hh:mm AM/PM'
                    if '|' in option_text:
                        date_part = option_text.split('|')[1].strip()
                        logger.info(f"📅 Parte de fecha extraída: '{date_part}'")
                        
                        # Parsear la fecha
                        try:
                            # Intentar diferentes formatos
                            event_date = None
                            formats_to_try = [
                                '%d/%m/%y %I:%M %p',  # dd/mm/yy hh:mm AM/PM
                                '%d/%m/%Y %I:%M %p',  # dd/mm/yyyy hh:mm AM/PM
                                '%d/%m/%y %H:%M',     # dd/mm/yy hh:mm
                                '%d/%m/%Y %H:%M'      # dd/mm/yyyy hh:mm
                            ]
                            
                            for fmt in formats_to_try:
                                try:
                                    event_date = datetime.strptime(date_part, fmt)
                                    logger.info(f"✅ Fecha parseada con formato '{fmt}': {event_date}")
                                    break
                                except ValueError:
                                    continue
                            
                            if event_date:
                                # Asegurar que la fecha tenga zona horaria
                                if event_date.tzinfo is None:
                                    event_date = buenos_aires_tz.localize(event_date)
                                else:
                                    event_date = event_date.astimezone(buenos_aires_tz)
                                
                                # Comparar con fecha actual
                                if event_date >= now_ba:
                                    future_events.append(event_option)
                                    logger.info(f"✅ Evento FUTURO: {event_date.strftime('%d/%m/%Y %I:%M %p')}")
                                    print(f"✅ FUTURO: {option_text}")
                                else:
                                    past_events.append(event_option)
                                    logger.info(f"❌ Evento PASADO: {event_date.strftime('%d/%m/%Y %I:%M %p')}")
                                    print(f"❌ PASADO: {option_text}")
                            else:
                                logger.warning(f"⚠️ No se pudo parsear fecha: '{date_part}'")
                                # Si no se puede parsear, considerar como futuro por seguridad
                                future_events.append(event_option)
                                print(f"⚠️ NO PARSEABLE (considerado futuro): {option_text}")
                                
                        except Exception as e:
                            logger.error(f"❌ Error parseando fecha '{date_part}': {str(e)}")
                            # Si hay error, considerar como futuro por seguridad
                            future_events.append(event_option)
                            print(f"❌ ERROR PARSEANDO (considerado futuro): {option_text}")
                    else:
                        logger.warning(f"⚠️ Formato inesperado, no contiene '|': '{option_text}'")
                        # Si no tiene el formato esperado, considerar como futuro por seguridad
                        future_events.append(event_option)
                        print(f"⚠️ FORMATO INESPERADO (considerado futuro): {option_text}")
                        
                except Exception as e:
                    logger.error(f"❌ Error analizando opción '{event_option['text']}': {str(e)}")
                    continue
            
            # Resumen del análisis
            logger.info(f"📊 RESUMEN DEL ANÁLISIS DE FECHAS:")
            logger.info(f"  📅 Total de eventos: {len(event_options)}")
            logger.info(f"  ✅ Eventos futuros: {len(future_events)}")
            logger.info(f"  ❌ Eventos pasados: {len(past_events)}")
            
            print(f"\n📊 RESUMEN DE FECHAS:")
            print(f"Total eventos: {len(event_options)}")
            print(f"Futuros: {len(future_events)}")
            print(f"Pasados: {len(past_events)}")
            
            # LÓGICA CORREGIDA: PROCESAR CADA EVENTO INDIVIDUALMENTE - TODOS LOS EVENTOS
            # Ya no distinguimos entre fechas pasadas y futuras - procesamos TODAS
            all_events = past_events + future_events
            
            if len(all_events) == 0:
                # No hay eventos en absoluto
                logger.warning("⚠️ NO hay eventos disponibles - Saltando este artista")
                print(f"⚠️ SALTANDO ARTISTA: No hay eventos disponibles")
                
                # Recargar la página para limpiar el input
                logger.info("🔄 Recargando página porque no hay eventos...")
                self.driver.refresh()
                time.sleep(5)
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                )
                logger.info("✅ Página cargada completamente después de no encontrar eventos")
                
                return False, []
                
            # PROCESAR CADA EVENTO INDIVIDUALMENTE - ASEGURAR QUE PROCESE TODOS
            logger.info(f"✅ PROCESANDO CADA EVENTO INDIVIDUALMENTE: {len(past_events)} pasadas + {len(future_events)} futuras = {len(all_events)} total")
            print(f"🎯 PROCESAMIENTO INDIVIDUAL: {len(all_events)} fechas ({len(past_events)} pasadas + {len(future_events)} futuras)")
            
            # PROCESAR TODAS LAS FECHAS INDIVIDUALMENTE - ASEGURAR QUE NO SE SALTE NINGUNA
            logger.info("✅ PROCESANDO TODAS LAS FECHAS - Una por una, sin saltarse ninguna")
            print(f"✅ PROCESANDO UNO POR UNO: {len(all_events)} eventos totales")
            
            # Procesar cada fecha individualmente - PROCESO COMPLETO DESDE CERO PARA CADA FECHA
            all_extracted_data = []
            remaining_dates = [event['text'] for event in all_events]  # Guardar TODAS las fechas a procesar
            
            logger.info(f"📋 LISTA COMPLETA DE EVENTOS A PROCESAR:")
            for idx, date_text in enumerate(remaining_dates):
                logger.info(f"  📋 Evento {idx+1}: '{date_text}'")
                print(f"📋 EVENTO {idx+1}: {date_text}")
            
            eventos_procesados_exitosamente = 0
            eventos_con_error = 0
            
            for i, target_date_text in enumerate(remaining_dates):
                try:
                    logger.info(f"🖱️ PROCESANDO EVENTO {i+1}/{len(remaining_dates)}: '{target_date_text}'")
                    print(f"🎯 PROCESANDO EVENTO {i+1}/{len(remaining_dates)}: {target_date_text}")
                    
                    # Para la primera fecha, el dropdown ya está abierto, solo necesitamos seleccionar la fecha
                    if i == 0:
                        logger.info("🎯 Primera fecha - dropdown ya está abierto, solo seleccionando fecha")
                        
                        # 3. Buscar y seleccionar la fecha específica (mismo código para ambos casos)
                        logger.info(f"🎯 Buscando fecha: '{target_date_text}'")
                        options = self.driver.find_elements(By.CSS_SELECTOR, ".mud-list-item")
                        
                        target_found = False
                        for option in options:
                            option_text = option.text.strip()
                            
                            # Buscar coincidencia exacta o parcial más inteligente
                            if (target_date_text == option_text or 
                                target_date_text in option_text or
                                option_text in target_date_text):
                                
                                logger.info(f"✅ Fecha encontrada, haciendo clic: '{option_text}' (buscaba: '{target_date_text}')")
                                option.click()
                                time.sleep(1)
                                target_found = True
                                break
                        
                        if not target_found:
                            logger.error(f"❌ No se encontró la fecha: '{target_date_text}'")
                            eventos_con_error += 1
                            continue
                            
                        # 4. Hacer clic afuera para cerrar dropdowns
                        self.driver.execute_script("document.body.click();")
                        time.sleep(2)
                        
                        # 5. Hacer clic en el botón "Buscar"
                        buscar_button = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, "/html/body/div[3]/div/div/div[1]/div[5]/button"))
                        )
                        self.driver.execute_script("arguments[0].click();", buscar_button)
                        logger.info("✅ Botón 'Buscar' clickeado para fecha individual")
                        
                        # 6. Esperar 30 segundos después de hacer clic en 'Buscar'
                        logger.info("⏳ Esperando 30 segundos después de hacer clic en 'Buscar'...")
                        print(f"⏳ ESPERANDO 30 SEGUNDOS para evento {i+1}...")
                        time.sleep(30)
                        
                        # 7. Extraer datos con desglose diario
                        logger.info("📊 Extrayendo datos con desglose diario...")
                        tabla_data = self.extract_ventas_funcion_data_with_daily_breakdown(target_date_text)
                        
                        if tabla_data:
                            all_extracted_data.extend(tabla_data)
                            eventos_procesados_exitosamente += 1
                            logger.info(f"✅ Datos extraídos para evento {i+1}: {len(tabla_data)} registros")
                            print(f"✅ DATOS EXTRAÍDOS para evento {i+1}")
                        else:
                            eventos_con_error += 1
                            logger.warning(f"⚠️ No se pudieron extraer datos para evento {i+1}")
                            print(f"⚠️ SIN DATOS para evento {i+1}")
                        
                        # 8. SIEMPRE recargar página después de extraer datos (excepto la última)
                        if i < len(remaining_dates) - 1:  # No recargar en la última fecha
                            logger.info("🔄 Recargando página después de extraer datos...")
                            self.driver.refresh()
                            time.sleep(5)
                            WebDriverWait(self.driver, 15).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                            )
                            logger.info("✅ Página recargada después de extraer datos")
                        
                    else:
                        # Para fechas 2, 3, 4, etc. hacer el proceso completo desde cero
                        logger.info(f"🔄 PROCESO COMPLETO DESDE CERO para evento {i+1}...")
                        
                        # 1. Escribir el artista desde cero
                        logger.info(f"⌨️ Escribiendo artista: '{original_artist_name}'")
                        option_found = self.try_manual_typing(original_artist_name, 0, [original_artist_name])
                        if not option_found:
                            logger.error(f"❌ No se pudo escribir artista '{original_artist_name}'")
                            eventos_con_error += 1
                            continue
                        
                        # 2. Abrir dropdown de eventos - USAR EL MISMO SELECTOR EXACTO QUE FUNCIONA
                        logger.info("🔍 Abriendo dropdown de eventos...")
                        try:
                            evento_input = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, "/html/body/div[3]/div/div/div[1]/div[4]/div/div/div/div[1]/input"))
                            )
                            logger.info("✅ Input de eventos encontrado con XPath específico")
                            evento_input.click()
                            logger.info("✅ Dropdown de eventos abierto correctamente")
                            time.sleep(3)
                        except Exception as e:
                            logger.error(f"❌ Error abriendo dropdown de eventos: {str(e)}")
                            eventos_con_error += 1
                            continue
                        
                        # 3. Buscar y seleccionar la fecha específica
                        logger.info(f"🎯 Buscando fecha: '{target_date_text}'")
                        options = self.driver.find_elements(By.CSS_SELECTOR, ".mud-list-item")
                        
                        # Debug: mostrar todas las opciones disponibles
                        logger.info(f"📋 Opciones disponibles en dropdown:")
                        for idx, opt in enumerate(options):
                            opt_text = opt.text.strip()
                            logger.info(f"  📋 Opción {idx+1}: '{opt_text}'")
                        
                        target_found = False
                        for option in options:
                            option_text = option.text.strip()
                            
                            # Buscar coincidencia exacta o parcial más inteligente
                            if (target_date_text == option_text or 
                                target_date_text in option_text or
                                option_text in target_date_text):
                                
                                logger.info(f"✅ Fecha encontrada, haciendo clic: '{option_text}' (buscaba: '{target_date_text}')")
                                option.click()
                                time.sleep(1)
                                target_found = True
                                break
                        
                        if not target_found:
                            logger.error(f"❌ No se encontró la fecha: '{target_date_text}'")
                            logger.error(f"❌ Opciones disponibles fueron: {[opt.text.strip() for opt in options]}")
                            eventos_con_error += 1
                            continue
                        
                        # 4. Hacer clic afuera para cerrar dropdowns
                        self.driver.execute_script("document.body.click();")
                        time.sleep(2)
                        
                        # 5. Hacer clic en el botón "Buscar"
                        buscar_button = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, "/html/body/div[3]/div/div/div[1]/div[5]/button"))
                        )
                        self.driver.execute_script("arguments[0].click();", buscar_button)
                        logger.info("✅ Botón 'Buscar' clickeado para fecha individual")
                        
                        # 6. Esperar 30 segundos después de hacer clic en 'Buscar'
                        logger.info("⏳ Esperando 30 segundos después de hacer clic en 'Buscar'...")
                        print(f"⏳ ESPERANDO 30 SEGUNDOS para evento {i+1}...")
                        time.sleep(30)
                        
                        logger.info("✅ Espera completada - Intentando extraer datos")
                        print(f"✅ TIEMPO COMPLETADO para evento {i+1} - Extrayendo datos...")
                        
                        # 7. Extraer datos de esta fecha con desglose diario
                        fecha_data = self.extract_ventas_funcion_data_with_daily_breakdown(target_date_text)
                        if fecha_data:
                            all_extracted_data.extend(fecha_data)
                            eventos_procesados_exitosamente += 1
                            logger.info(f"✅ Datos extraídos para evento {i+1}")
                            print(f"✅ DATOS EXTRAÍDOS para evento {i+1}")
                        else:
                            eventos_con_error += 1
                            logger.warning(f"⚠️ No se pudieron extraer datos para evento {i+1}")
                            print(f"⚠️ SIN DATOS para evento {i+1}")
                        
                        # 8. SIEMPRE recargar página después de extraer datos (excepto la última)
                        if i < len(remaining_dates) - 1:  # No recargar en la última fecha
                            logger.info("🔄 Recargando página después de extraer datos...")
                            self.driver.refresh()
                            time.sleep(5)
                            WebDriverWait(self.driver, 15).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                            )
                            logger.info("✅ Página recargada después de extraer datos")
                            
                except Exception as e:
                    logger.error(f"❌ Error procesando evento {i+1}: {str(e)}")
                    eventos_con_error += 1
                    # Si hay error, recargar página para el siguiente intento
                    if i < len(remaining_dates) - 1:
                        try:
                            logger.info("🔄 Recargando página por error...")
                            self.driver.refresh()
                            time.sleep(5)
                            WebDriverWait(self.driver, 15).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
                            )
                        except:
                            pass
                    continue
            
            # RESUMEN FINAL DEL PROCESAMIENTO DE ESTE ARTISTA
            logger.info("=" * 60)
            logger.info(f"📊 RESUMEN FINAL PARA ARTISTA '{original_artist_name}':")
            logger.info(f"  📋 Total de eventos: {len(remaining_dates)}")
            logger.info(f"  ✅ Eventos procesados exitosamente: {eventos_procesados_exitosamente}")
            logger.info(f"  ❌ Eventos con error: {eventos_con_error}")
            logger.info(f"  📊 Total de datos extraídos: {len(all_extracted_data)} registros")
            logger.info("=" * 60)
            
            print(f"📊 RESUMEN - {original_artist_name}: {eventos_procesados_exitosamente}/{len(remaining_dates)} eventos procesados")
            
            # Si se extrajeron datos, devolver todos los datos acumulados
            if all_extracted_data:
                return True, all_extracted_data
            else:
                return False, []
                
        except Exception as e:
            logger.error(f"❌ Error procesando opción '{option_text}': {str(e)}")
            
            # Recargar la página para limpiar el input
            logger.info("🔄 Recargando página después de error general...")
            self.driver.refresh()
            time.sleep(5)
            # Esperar a que la página cargue completamente
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.mud-select.mud-autocomplete"))
            )
            logger.info("✅ Página cargada completamente después de error general")
            
            return False, []
    
    def get_fresh_future_events(self):
        """
        MODIFICADO: Obtiene TODOS los eventos (sin filtro de fecha) después de recargar la página
        
        Returns:
            list: Lista de TODOS los eventos con elementos frescos
        """
        try:
            # El dropdown ya debería estar abierto, pero verificar que tenemos opciones
            logger.info("🔍 Obteniendo TODOS los eventos frescos del dropdown ya abierto...")
            
            # No necesitamos abrir el dropdown de nuevo, ya está abierto
            # Solo obtener las opciones directamente
            
            # Obtener todas las opciones visibles
            options = self.driver.find_elements(By.CSS_SELECTOR, ".mud-list-item")
            
            all_events = []
            
            for option in options:
                option_text = option.text.strip()
                
                # Saltar "Seleccionar todos"
                if "seleccionar todos" in option_text.lower():
                    continue
                
                # Incluir TODOS los eventos (sin filtro de fecha)
                all_events.append({
                    'text': option_text,
                    'element': option
                })
            
            logger.info(f"✅ TODOS los eventos frescos encontrados: {len(all_events)}")
            return all_events
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo eventos frescos: {str(e)}")
            return []

    def reselect_artist_for_next_date(self, option_text, current_index):
        """
        Reselecciona el artista después de recargar la página para procesar la siguiente fecha
        
        Args:
            option_text (str): Nombre del artista a reseleccionar
            current_index (int): Índice actual de la fecha que se está procesando
            
        Returns:
            bool: True si se reseleccionó exitosamente, False en caso contrario
        """
        try:
            logger.info(f"🔄 Reseleccionando artista '{option_text}' para siguiente fecha...")
            
            # Buscar el primer input y escribir el artista
            divs = self.driver.find_elements(By.CSS_SELECTOR, "div.mud-select.mud-autocomplete")
            if divs:
                first_div = divs[0]
                input_element = first_div.find_element(By.CSS_SELECTOR, "input.mud-input-slot")
                
                # Hacer scroll y clic
                self.driver.execute_script("arguments[0].scrollIntoView(true);", input_element)
                time.sleep(0.5)
                self.driver.execute_script("arguments[0].click();", input_element)
                time.sleep(1)
                
                # Limpiar y escribir el artista
                input_element.clear()
                time.sleep(0.5)
                
                # Escribir carácter por carácter
                for char in option_text:
                    input_element.send_keys(char)
                    time.sleep(0.05)
                
                time.sleep(2)
                
                # Buscar y hacer clic en la opción del artista
                options = self.driver.find_elements(By.CSS_SELECTOR, ".mud-list-item")
                found_option = False
                logger.info(f"🔍 Buscando '{option_text}' entre {len(options)} opciones disponibles")
                
                for i, option in enumerate(options):
                    option_text_clean = option.text.strip()
                    logger.info(f"  📋 Opción {i+1}: '{option_text_clean}'")
                    if option_text_clean == option_text or option_text in option_text_clean:
                        self.driver.execute_script("arguments[0].click();", option)
                        logger.info(f"✅ Artista '{option_text}' reseleccionado (coincidencia: '{option_text_clean}')")
                        time.sleep(2)
                        found_option = True
                        break
                
                if not found_option:
                    logger.error(f"❌ No se encontró la opción '{option_text}' en el dropdown")
                    return False
                
                # PASO 4: Después de seleccionar el artista, abrir el dropdown de eventos
                logger.info("🔍 Abriendo dropdown de eventos después de reseleccionar artista...")
                
                # Esperrar un poco para que la página se estabilice
                time.sleep(2)
                
                # DEBUG: Ver todos los elementos disponibles
                try:
                    all_inputs = self.driver.find_elements(By.TAG_NAME, "input")
                    logger.info(f"🔍 DEBUG: Encontrados {len(all_inputs)} elementos <input> en la página")
                    for i, inp in enumerate(all_inputs[:5]):  # Solo mostrar los primeros 5
                        try:
                            placeholder = inp.get_attribute("placeholder") or "sin placeholder"
                            class_name = inp.get_attribute("class") or "sin clase"
                            logger.info(f"  📋 Input {i+1}: placeholder='{placeholder}', class='{class_name}'")
                        except:
                            logger.info(f"  📋 Input {i+1}: No se pudo obtener atributos")
                except Exception as e:
                    logger.warning(f"⚠️ Error obteniendo debug info: {str(e)}")
                
                # Intentar diferentes selectores para el dropdown de eventos
                event_selectors = [
                    "//input[contains(@class, 'mud-input-slot')]",  # Selector más flexible
                    "//input[contains(@class, 'mud-select-input')]",  # Específico para selects
                    ".mud-input-slot",  # CSS selector
                    ".mud-select-input"  # CSS selector para selects
                ]
                
                event_input = None
                for i, selector in enumerate(event_selectors):
                    try:
                        # Alternar entre XPath y CSS selector
                        if selector.startswith('//'):
                            event_inputs = self.driver.find_elements(By.XPATH, selector)
                        else:
                            event_inputs = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            
                        logger.info(f"🔍 Selector {i+1} ('{selector}') encontró {len(event_inputs)} elementos")
                        
                        if event_inputs:
                            # Lógica inteligente para elegir el input correcto
                            if len(event_inputs) >= 2:
                                # Si hay 2 o más, usar el segundo (el de eventos)
                                event_input = event_inputs[1]
                                logger.info(f"✅ Usando el segundo elemento (evento dropdown)")
                            elif len(event_inputs) == 1:
                                event_input = event_inputs[0]
                                logger.info(f"✅ Usando el único elemento disponible")
                            
                            logger.info(f"✅ Input de eventos encontrado con selector {i+1}")
                            break
                    except Exception as e:
                        logger.warning(f"⚠️ Selector {i+1} falló: {str(e)}")
                        continue
                
                if event_input:
                    try:
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", event_input)
                        time.sleep(0.5)
                        self.driver.execute_script("arguments[0].click();", event_input)
                        logger.info("✅ Dropdown de eventos abierto")
                        time.sleep(3)  # Dar tiempo para que se abran las opciones
                        return True
                    except Exception as e:
                        logger.error(f"❌ Error clickeando dropdown de eventos: {str(e)}")
                        return False
                else:
                    logger.error("❌ No se pudo encontrar el dropdown de eventos con ningún selector")
                    return False
                        
            else:
                logger.error("❌ No se encontraron divs para reseleccionar artista")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error reseleccionando artista: {str(e)}")
            return False

    def close_driver(self):
        """Cierra el driver del navegador"""
        if self.driver:
            self.driver.quit()
            logger.info("🔒 Driver cerrado")

def main():
    """Función principal para ejecutar el scraper"""
    logger.info("🚀 INICIANDO SCRAPER DE MOVISTAR ARENA - VENTAS POR FUNCIÓN")
    logger.info("=" * 50)
    
    scraper = MovistarArenaVentasFuncionScraper(headless=True)
    
    try:
        # Configurar driver
        logger.info("🔧 PASO 1: Configurando driver...")
        if not scraper.setup_driver():
            logger.error("❌ No se pudo configurar el driver")
            return
        logger.info("✅ Driver configurado exitosamente")
        
        # Navegar a la página
        logger.info("🌐 PASO 2: Navegando a la página...")
        if not scraper.navigate_to_page():
            logger.error("❌ No se pudo navegar a la página")
            return
        logger.info("✅ Navegación exitosa")
        
        # Analizar estructura de la página
        logger.info("🔍 PASO 3: Analizando estructura de la página...")
        if not scraper.analyze_page_structure():
            logger.error("❌ No se pudo analizar la estructura de la página")
            return
        logger.info("✅ Análisis de estructura completado")
        
        # Verificar si se requiere login
        logger.info("🔐 PASO 4: Verificando requisitos de login...")
        login_required = scraper.check_login_required()
        
        # Si se requiere login, ejecutar el proceso de autenticación
        if login_required:
            logger.info("🔐 PASO 5: Ejecutando proceso de login...")
            if scraper.perform_login():
                logger.info("✅ Login exitoso - Continuando con el análisis...")
                
                # No más screenshots automáticos
                logger.info("✅ Login exitoso sin tomar screenshot")
                
                # Analizar estructura de la página después del login
                logger.info("🔍 PASO 6: Analizando estructura de la página después del login...")
                if not scraper.analyze_page_structure():
                    logger.error("❌ No se pudo analizar la estructura de la página después del login")
                    return
                logger.info("✅ Análisis de estructura después del login completado")
                
            else:
                logger.error("❌ Error en el proceso de login")
                return
        else:
            logger.info("✅ No se requiere login - continuando con el análisis...")
        
        # PASO 7: Obtener todas las opciones del primer input y agruparlas por artista
        logger.info("🔍 PASO 7: Obteniendo todas las opciones del primer input...")
        try:
            # Obtener todas las opciones disponibles
            all_options = scraper.get_first_input_options()
            
            if not all_options:
                logger.error("❌ No se pudieron obtener las opciones del primer input")
                return
            
            logger.info(f"📋 Total de opciones encontradas: {len(all_options)}")
            print(f"TOTAL DE OPCIONES ENCONTRADAS: {len(all_options)}")
            
            # Agrupar opciones por artista
            artist_groups = scraper.group_options_by_artist(all_options)
            
            if not artist_groups:
                logger.error("❌ No se pudieron agrupar las opciones por artista")
                return
            
            logger.info(f"🎤 Total de artistas únicos a procesar: {len(artist_groups)}")
            print(f"TOTAL DE ARTISTAS ÚNICOS: {len(artist_groups)}")
            
            # Procesar cada grupo de artista
            successful_artists = 0
            failed_artists = 0
            processed_artists = set()  # Para evitar procesar el mismo artista múltiples veces
            
            for artist_index, (artist_name, artist_options) in enumerate(artist_groups.items()):
                try:
                    # Verificar si ya procesamos este artista (comparación case-insensitive)
                    artist_name_normalized = artist_name.upper()
                    if artist_name_normalized in processed_artists:
                        logger.info(f"⏭️ SALTANDO ARTISTA {artist_index + 1}/{len(artist_groups)}: '{artist_name}' (ya procesado - duplicado case-insensitive)")
                        print(f"SALTADO - ARTISTA {artist_index + 1}: {artist_name} (duplicado case-insensitive)")
                        continue
                    
                    # FILTRO: Verificar si el artista está en la lista de artistas permitidos
                    if not scraper.is_artist_allowed(artist_name):
                        logger.info(f"🚫 SALTANDO ARTISTA {artist_index + 1}/{len(artist_groups)}: '{artist_name}' (NO está en la lista de artistas permitidos)")
                        print(f"🚫 FILTRADO - ARTISTA {artist_index + 1}: {artist_name} (no está en la lista)")
                        continue
                    
                    logger.info(f"🚀 INICIANDO PROCESAMIENTO DE ARTISTA {artist_index + 1}/{len(artist_groups)}: '{artist_name}' ✅ PERMITIDO")
                    logger.info(f"📋 Opciones para este artista: {len(artist_options)}")
                    
                    # DEBUG específico para ERREWAY
                    if "erreway" in artist_name.lower() or "errewey" in artist_name.lower():
                        logger.info(f"🔍 DEBUG ERREWAY - Iniciando procesamiento específico para: '{artist_name}'")
                        logger.info(f"🔍 DEBUG ERREWAY - Opciones disponibles: {artist_options}")
                    
                    # Marcar el artista como procesado (usar versión normalizada)
                    processed_artists.add(artist_name_normalized)
                    
                    # Procesar todas las opciones del artista
                    success = scraper.process_artist_group(artist_name, artist_options)
                    
                    # DEBUG específico para ERREWAY - resultado
                    if "erreway" in artist_name.lower() or "errewey" in artist_name.lower():
                        logger.info(f"🔍 DEBUG ERREWAY - Resultado del procesamiento: {'ÉXITO' if success else 'ERROR'}")
                    
                    if success:
                        successful_artists += 1
                        logger.info(f"✅ Artista '{artist_name}' procesado exitosamente")
                        print(f"COMPLETADO - ARTISTA {artist_index + 1}: {artist_name}")
                    else:
                        failed_artists += 1
                        logger.error(f"❌ Error procesando artista '{artist_name}'")
                        print(f"ERROR - ARTISTA {artist_index + 1}: {artist_name}")
                    
                    # No hay pausa entre artistas - continuar inmediatamente
                        
                except Exception as e:
                    failed_artists += 1
                    logger.error(f"❌ Error crítico procesando artista '{artist_name}': {str(e)}")
                    print(f"ERROR CRÍTICO - ARTISTA {artist_index + 1}: {artist_name}")
                    continue
            
            # Calcular estadísticas del filtrado
            total_artistas_encontrados = len(artist_groups)
            artistas_filtrados = total_artistas_encontrados - successful_artists - failed_artists
            
            # Resumen final del procesamiento
            logger.info("=" * 80)
            logger.info("📊 RESUMEN FINAL DEL PROCESAMIENTO CON FILTRO:")
            logger.info(f"  🎤 Total de artistas encontrados: {total_artistas_encontrados}")
            logger.info(f"  🚫 Artistas filtrados (no en lista): {artistas_filtrados}")
            logger.info(f"  ✅ Artistas procesados exitosamente: {successful_artists}")
            logger.info(f"  ❌ Artistas con error: {failed_artists}")
            if successful_artists + failed_artists > 0:
                logger.info(f"  📈 Tasa de éxito (artistas permitidos): {(successful_artists/(successful_artists + failed_artists)*100):.1f}%")
            logger.info("🎯 ARTISTAS PERMITIDOS PROCESADOS:")
            for i, artista in enumerate(scraper.artistas_permitidos, 1):
                if artista == "Diego Torres":
                    logger.info(f"    {i}. {artista} (OPCIÓN 2)")
                else:
                    logger.info(f"    {i}. {artista}")
            
            # Actualizar datos finales del scraper
            scraper.final_data["total_artistas_procesados"] = len(artist_groups)
            scraper.final_data["artistas_exitosos"] = successful_artists
            scraper.final_data["artistas_con_error"] = failed_artists
            # Restar 3 horas directamente para hora de Argentina (UTC-3)
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            scraper.final_data["fecha_extraccion"] = fecha_extraccion_utc3.isoformat()
            
            print(f"\n" + "="*60)
            print("RESUMEN FINAL")
            print("="*60)
            print(f"Total de artistas: {len(artist_groups)}")
            print(f"Exitosos: {successful_artists}")
            print(f"Con errores: {failed_artists}")
            print(f"Tasa de éxito: {(successful_artists/len(artist_groups)*100):.1f}%")
            
            for i, (artist_name, options) in enumerate(artist_groups.items()):
                status = "✅" if i < successful_artists else "❌"
                print(f"{status} Artista {i+1}: {artist_name} ({len(options)} opciones)")
                    
        except Exception as e:
            logger.error(f"❌ Error en el proceso de iteración de artistas: {str(e)}")
        
        # Resumen final
        logger.info("=" * 50)
        logger.info("📊 RESUMEN DEL PROCESO:")
        logger.info(f"  🔐 Login requerido: {'Sí' if login_required else 'No'}")
        logger.info("  🔍 Iteración por opciones: Completada")
        logger.info("  ✅ Proceso completado exitosamente")
        
        if login_required:
            logger.info("✅ Login implementado y ejecutado exitosamente")
            logger.info("🌐 Página accesible después de la autenticación")
        
        logger.info("🎯 Todas las opciones del primer input han sido procesadas")
        logger.info("📊 Datos JSON preparados en memoria para cada opción exitosa")
        
        # Retornar los datos finales para Airflow
        return scraper.final_data
        
    except Exception as e:
        logger.error(f"❌ Error en el proceso principal: {str(e)}")
        logger.error("🔍 Revisa los logs para más detalles")
        return None
    finally:
        scraper.close_driver()
        logger.info("🏁 SCRAPER FINALIZADO")

def run_scraper_for_airflow():
    """
    Función específica para ejecutar desde Airflow
    Retorna los datos extraídos en formato JSON para enviar a base de datos
    
    Returns:
        dict: Datos completos extraídos o None si hay error
    """
    try:
        logger.info("🚀 INICIANDO SCRAPER MOVISTAR ARENA VENTAS POR FUNCIÓN PARA AIRFLOW")
        result = main()
        return result
    except Exception as e:
        logger.error(f"❌ Error ejecutando scraper para Airflow: {str(e)}")
        return None

if __name__ == "__main__":
    main()
