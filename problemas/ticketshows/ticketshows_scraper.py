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
from datetime import datetime
import json
import re

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TicketShowsScraper:
    def __init__(self, headless=False):
        """
        Inicializa el scraper de TicketShows
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless
        """
        self.driver = None
        self.headless = headless
        self.base_url = "https://reportes.ticketshow.com.ec/"  # URL base de ticketshows
        
        # Lista de credenciales para probar una por una
        self.credentials = [
            {"username": "dukiec", "password": "2025"},
            {"username": "relsbec", "password": "2025"}
        ]
        self.current_credential_index = 0
        
        # Crear carpeta para descargas
        self.download_folder = "ticketshows/jsonticketshows"
        self.setup_download_folder()
        
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
        """Configura el driver de Chrome con las opciones necesarias"""
        try:
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument("--headless")
            
            # Opciones básicas para Windows
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            
            # Configurar user agent
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
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
            
            logger.info("Driver de Chrome configurado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error al configurar el driver: {str(e)}")
            return False
    
    def navigate_to_login(self):
        """Navega a la página de login de TicketShows"""
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
            
            # Buscar todos los selects
            selects = self.driver.find_elements(By.TAG_NAME, "select")
            logger.info(f"Total de selects encontrados: {len(selects)}")
            
            for i, select_elem in enumerate(selects):
                try:
                    select_name = select_elem.get_attribute("name")
                    select_id = select_elem.get_attribute("id")
                    select_class = select_elem.get_attribute("class")
                    
                    logger.info(f"Select {i+1}: name='{select_name}', id='{select_id}', class='{select_class}'")
                except:
                    continue
            
            logger.info("=== FIN DEBUG ===")
            
        except Exception as e:
            logger.error(f"Error en debug: {str(e)}")
    
    def debug_venta_diaria_page(self):
        """Debug específico para la página de Venta Diaria"""
        try:
            logger.info("=== DEBUG: Analizando página de Venta Diaria ===")
            
            # Obtener información de la página
            current_url = self.driver.current_url
            page_title = self.driver.title
            logger.info(f"URL actual: {current_url}")
            logger.info(f"Título de la página: {page_title}")
            
            # Buscar frames/iframes
            try:
                frames = self.driver.find_elements(By.TAG_NAME, "iframe")
                logger.info(f"Encontrados {len(frames)} frames en la página")
                
                for i, frame in enumerate(frames):
                    try:
                        frame_name = frame.get_attribute("name")
                        frame_id = frame.get_attribute("id")
                        frame_src = frame.get_attribute("src")
                        logger.info(f"Frame {i+1}: name='{frame_name}', id='{frame_id}', src='{frame_src}'")
                    except:
                        continue
            except Exception as e:
                logger.warning(f"Error analizando frames: {str(e)}")
            
            # Buscar elementos específicos de la página de Venta Diaria
            actividades_encontradas = []
            try:
                # Buscar el select de actividades
                actividades_select = self.driver.find_element(By.ID, "cmb_actividades")
                logger.info("✅ Select de actividades encontrado por ID")
                
                # Obtener opciones
                opciones = actividades_select.find_elements(By.TAG_NAME, "option")
                logger.info(f"Total de opciones en el select: {len(opciones)}")
                
                for i, opcion in enumerate(opciones):
                    try:
                        valor = opcion.get_attribute("value")
                        texto = opcion.text.strip()
                        seleccionada = opcion.get_attribute("selected")
                        
                        actividad_info = {
                            'indice': i,
                            'valor': valor,
                            'texto': texto,
                            'seleccionada': seleccionada == "true"
                        }
                        actividades_encontradas.append(actividad_info)
                        
                        logger.info(f"Opción {i+1}: valor='{valor}', texto='{texto}'")
                    except:
                        continue
                        
            except Exception as e:
                logger.warning(f"No se pudo encontrar el select de actividades: {str(e)}")
                
                # Intentar buscar por otros métodos
                try:
                    selects = self.driver.find_elements(By.TAG_NAME, "select")
                    logger.info(f"Encontrados {len(selects)} elementos select en la página")
                    
                    for i, select_elem in enumerate(selects):
                        try:
                            select_name = select_elem.get_attribute("name")
                            select_id = select_elem.get_attribute("id")
                            logger.info(f"Select {i+1}: name='{select_name}', id='{select_id}'")
                        except:
                            continue
                except:
                    logger.error("No se encontraron elementos select en la página")
                
                # Buscar otros elementos que puedan estar en la página
                try:
                    # Buscar formularios
                    forms = self.driver.find_elements(By.TAG_NAME, "form")
                    logger.info(f"Encontrados {len(forms)} formularios en la página")
                    
                    # Buscar divs con contenido
                    divs = self.driver.find_elements(By.TAG_NAME, "div")
                    logger.info(f"Encontrados {len(divs)} elementos div en la página")
                    
                    # Buscar elementos con texto que contenga "actividad" o "venta"
                    for i, div in enumerate(divs[:10]):  # Solo los primeros 10 para no saturar
                        try:
                            div_text = div.text.strip()
                            if div_text and ("actividad" in div_text.lower() or "venta" in div_text.lower()):
                                logger.info(f"Div {i+1} con texto relevante: {div_text[:100]}...")
                        except:
                            continue
                            
                except Exception as e:
                    logger.warning(f"Error analizando otros elementos: {str(e)}")
            
            logger.info("=== FIN DEBUG VENTA DIARIA ===")
            return actividades_encontradas
            
        except Exception as e:
            logger.error(f"Error en debug de Venta Diaria: {str(e)}")
            return []
    
    def wait_for_login_form(self):
        """Espera a que cargue el formulario de login"""
        try:
            logger.info("Esperando a que cargue el formulario de login...")
            
            # Esperar más tiempo para que la página cargue completamente
            time.sleep(5)
            
            # Usar los selectores específicos de ticketshows basados en el HTML proporcionado
            username_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "txtUsuario"))
            )
            logger.info("Campo de usuario encontrado con selector: (By.ID, 'txtUsuario')")
            
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "txtClave"))
            )
            logger.info("Campo de contraseña encontrado con selector: (By.ID, 'txtClave')")
            
            logger.info("Formulario de login cargado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error esperando el formulario de login: {str(e)}")
            return False
    
    def login(self, username, password):
        """Realiza el login con las credenciales proporcionadas"""
        try:
            logger.info("Iniciando proceso de login...")
            
            # Esperar a que el formulario esté listo
            if not self.wait_for_login_form():
                return False
            
            # Buscar y llenar el campo de username usando el selector correcto
            username_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "txtUsuario"))
            )
            username_field.clear()
            username_field.send_keys(username)
            logger.info(f"Usuario ingresado: {username}")
            
            # Buscar y llenar el campo de contraseña
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "txtClave"))
            )
            password_field.clear()
            password_field.send_keys(password)
            logger.info("Contraseña ingresada")
            
            # Buscar y hacer clic en el botón "Ingresar" usando el selector correcto
            login_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "btnIngresar"))
            )
            login_button.click()
            logger.info("Botón Ingresar clickeado")
            
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
    
    def login_with_specific_credential(self, credential_index):
        """Realiza el login con una credencial específica"""
        if credential_index >= len(self.credentials):
            logger.error(f"Índice de credencial inválido: {credential_index}")
            return False
            
        credential = self.credentials[credential_index]
        username = credential["username"]
        password = credential["password"]
        
        logger.info(f"Intentando login con credencial {credential_index + 1}/{len(self.credentials)}: {username}")
        
        # Intentar login con esta credencial
        if self.login(username, password):
            self.current_credential_index = credential_index
            logger.info(f"Login exitoso con credencial: {username}")
            return True
        else:
            logger.warning(f"Login falló con credencial: {username}")
            return False
    
    def get_current_credential_info(self):
        """Obtiene información sobre la credencial actualmente en uso"""
        if self.current_credential_index < len(self.credentials):
            current_cred = self.credentials[self.current_credential_index]
            return f"{current_cred['username']}"
        return "Ninguna"
    
    def run_with_credential(self, credential_index):
        """Ejecuta el scraper completo con una credencial específica"""
        try:
            credential = self.credentials[credential_index]
            username = credential["username"]
            
            logger.info(f"=== INICIANDO PROCESO CON CREDENCIAL {credential_index + 1}/{len(self.credentials)}: {username} ===")
            
            # Navegar a la página de login
            if not self.navigate_to_login():
                logger.error(f"No se pudo navegar a la página de login para {username}")
                return False
            
            # Realizar login con credencial específica
            if not self.login_with_specific_credential(credential_index):
                logger.error(f"No se pudo realizar el login con {username}")
                return False
            
            # Obtener información después del login
            new_page_info = self.get_page_info()
            if new_page_info:
                logger.info(f"Después del login - Título: {new_page_info['title']}")
                logger.info(f"Después del login - URL: {new_page_info['url']}")
            
            # Navegar al menú de Reportes
            logger.info("Navegando a Reportes...")
            if self.click_reportes_menu():
                logger.info("Menú de Reportes abierto")
                
                # Hacer clic en Venta Diaria
                if self.click_venta_diaria():
                    logger.info("Venta Diaria seleccionado")
                    
                    # Obtener actividades disponibles
                    actividades = self.debug_venta_diaria_page()
                    
                    if actividades:
                        logger.info(f"Encontradas {len(actividades)} actividades")
                        
                        # Procesar cada actividad y extraer datos de tabla
                        processed_count = 0
                        for actividad in actividades:
                            if actividad['valor'] != '0':  # Excluir la opción por defecto
                                logger.info(f"Procesando: {actividad['texto']} (ID: {actividad['valor']})")
                                
                                # Seleccionar la actividad
                                if self.select_actividad(actividad['valor']):
                                    logger.info(f"Actividad seleccionada: {actividad['texto']}")
                                    
                                    # Extraer datos de la tabla
                                    datos_tabla = self.get_table_data()
                                    
                                    if datos_tabla:
                                        logger.info(f"Extraídos {len(datos_tabla)} registros de la tabla")
                                        
                                        # Guardar datos en JSON con información de credencial
                                        json_file = self.save_table_data_to_json_with_credential(datos_tabla, actividad, username)
                                        if json_file:
                                            logger.info(f"Datos guardados en: {json_file}")
                                            processed_count += 1
                                        else:
                                            logger.warning("Error guardando datos de tabla")
                                    else:
                                        logger.warning("No se encontraron datos en la tabla")
                                else:
                                    logger.warning(f"Error seleccionando actividad: {actividad['texto']}")
                        
                        logger.info(f"Procesamiento completado. Se procesaron {processed_count} actividades.")
                    else:
                        logger.warning("No se encontraron actividades")
                        
                        # Intentar métodos alternativos
                        self.switch_to_default_content()
                        actividades = self.get_actividades_dropdown()
                        
                        if actividades:
                            logger.info(f"Encontradas {len(actividades)} actividades con método alternativo")
                            # Aquí se podría repetir el proceso de extracción
                        else:
                            logger.error("No se pudieron obtener actividades con ningún método")
                            return False
                else:
                    logger.error("No se pudo seleccionar Venta Diaria")
                    return False
            else:
                logger.error("No se pudo abrir el menú de Reportes")
                return False
            
            logger.info(f"=== PROCESO COMPLETADO EXITOSAMENTE CON {username} ===")
            return True
            
        except Exception as e:
            logger.error(f"Error ejecutando scraper con {username}: {str(e)}")
            return False
    
    def close(self):
        """Cierra el driver del navegador"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")
    
    def switch_to_default_content(self):
        """Vuelve al contexto principal del navegador"""
        try:
            self.driver.switch_to.default_content()
            logger.info("✅ Cambiado al contexto principal del navegador")
            return True
        except Exception as e:
            logger.error(f"Error cambiando al contexto principal: {str(e)}")
            return False
    
    def click_reportes_menu(self):
        """Hace clic en el menú de Reportes"""
        try:
            logger.info("Buscando menú de Reportes...")
            
            # Buscar el enlace de Reportes usando el selector específico
            reportes_menu = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a.nav-link.dropdown-toggle[data-toggle='dropdown']"))
            )
            
            # Hacer clic en el menú de Reportes
            reportes_menu.click()
            logger.info("✅ Menú de Reportes clickeado")
            
            # Esperar un momento para que se despliegue el menú
            time.sleep(2)
            
            return True
            
        except Exception as e:
            logger.error(f"Error haciendo clic en menú de Reportes: {str(e)}")
            return False
    
    def click_venta_diaria(self):
        """Hace clic en 'Venta Diaria' del menú de reportes"""
        try:
            logger.info("Buscando opción 'Venta Diaria'...")
            
            # Buscar el enlace de Venta Diaria
            venta_diaria_link = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a.dropdown-item[href='ventaDiaria.aspx']"))
            )
            
            # Hacer clic en Venta Diaria
            venta_diaria_link.click()
            logger.info("✅ Venta Diaria clickeado")
            
            # Esperar 15 segundos como solicitado
            logger.info("Esperando 15 segundos para que cargue la página...")
            time.sleep(15)
            
            # Verificar si hay frames/iframes y cambiar a ellos si es necesario
            try:
                frames = self.driver.find_elements(By.TAG_NAME, "iframe")
                logger.info(f"Encontrados {len(frames)} frames en la página")
                
                for i, frame in enumerate(frames):
                    try:
                        frame_name = frame.get_attribute("name")
                        frame_id = frame.get_attribute("id")
                        frame_src = frame.get_attribute("src")
                        logger.info(f"Frame {i+1}: name='{frame_name}', id='{frame_id}', src='{frame_src}'")
                        
                        # Si el frame tiene un nombre o ID relacionado con contenido, cambiar a él
                        if frame_name and ("content" in frame_name.lower() or "main" in frame_name.lower()):
                            logger.info(f"Cambiando al frame: {frame_name}")
                            self.driver.switch_to.frame(frame)
                            break
                        elif frame_id and ("content" in frame_id.lower() or "main" in frame_id.lower()):
                            logger.info(f"Cambiando al frame: {frame_id}")
                            self.driver.switch_to.frame(frame)
                            break
                    except Exception as e:
                        logger.warning(f"Error analizando frame {i+1}: {str(e)}")
                        continue
                
                # Si no se encontró un frame específico, intentar con el primero
                if len(frames) > 0:
                    try:
                        logger.info("Cambiando al primer frame disponible")
                        self.driver.switch_to.frame(frames[0])
                    except Exception as e:
                        logger.warning(f"Error cambiando al primer frame: {str(e)}")
                
            except Exception as e:
                logger.warning(f"Error manejando frames: {str(e)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error haciendo clic en Venta Diaria: {str(e)}")
            return False
    
    def get_actividades_dropdown(self):
        """Obtiene la información del desplegable de actividades"""
        try:
            logger.info("Buscando desplegable de actividades...")
            
            # Intentar diferentes estrategias para encontrar el elemento
            actividades_select = None
            
            # Estrategia 1: Buscar por ID
            try:
                actividades_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "cmb_actividades"))
                )
                logger.info("✅ Elemento encontrado por ID")
            except:
                logger.warning("No se encontró por ID, intentando por CSS selector...")
                
                # Estrategia 2: Buscar por CSS selector
                try:
                    actividades_select = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "select[name='cmb_actividades']"))
                    )
                    logger.info("✅ Elemento encontrado por CSS selector")
                except:
                    logger.warning("No se encontró por CSS selector, intentando por nombre...")
                    
                    # Estrategia 3: Buscar por nombre
                    try:
                        actividades_select = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.NAME, "cmb_actividades"))
                        )
                        logger.info("✅ Elemento encontrado por nombre")
                    except:
                        logger.error("No se pudo encontrar el elemento del desplegable")
                        return []
            
            if not actividades_select:
                return []
            
            logger.info("✅ Desplegable de actividades encontrado")
            
            # Obtener todas las opciones del select
            opciones = actividades_select.find_elements(By.TAG_NAME, "option")
            
            actividades = []
            for i, opcion in enumerate(opciones):
                try:
                    valor = opcion.get_attribute("value")
                    texto = opcion.text.strip()
                    seleccionada = opcion.get_attribute("selected")
                    
                    actividad_info = {
                        'indice': i,
                        'valor': valor,
                        'texto': texto,
                        'seleccionada': seleccionada == "true"
                    }
                    
                    actividades.append(actividad_info)
                    
                    logger.info(f"Opción {i}: valor='{valor}', texto='{texto}', seleccionada={seleccionada}")
                    
                except Exception as e:
                    logger.error(f"Error procesando opción {i}: {str(e)}")
                    continue
            
            logger.info(f"✅ Total de actividades encontradas: {len(actividades)}")
            return actividades
            
        except Exception as e:
            logger.error(f"Error obteniendo desplegable de actividades: {str(e)}")
            return []
    
    def click_actividades_dropdown(self):
        """Hace clic en el desplegable de actividades para abrirlo"""
        try:
            logger.info("Abriendo desplegable de actividades...")
            
            # Esperar un poco más para que la página cargue completamente
            time.sleep(3)
            
            # Intentar diferentes estrategias para encontrar el elemento
            actividades_select = None
            
            # Estrategia 1: Buscar por ID
            try:
                actividades_select = WebDriverWait(self.driver, 15).until(
                    EC.element_to_be_clickable((By.ID, "cmb_actividades"))
                )
                logger.info("✅ Elemento encontrado por ID")
            except:
                logger.warning("No se encontró por ID, intentando por CSS selector...")
                
                # Estrategia 2: Buscar por CSS selector
                try:
                    actividades_select = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "select[name='cmb_actividades']"))
                    )
                    logger.info("✅ Elemento encontrado por CSS selector")
                except:
                    logger.warning("No se encontró por CSS selector, intentando por nombre...")
                    
                    # Estrategia 3: Buscar por nombre
                    try:
                        actividades_select = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.NAME, "cmb_actividades"))
                        )
                        logger.info("✅ Elemento encontrado por nombre")
                    except:
                        logger.error("No se pudo encontrar el elemento del desplegable")
                        return False
            
            if actividades_select:
                # Hacer clic para abrir el desplegable
                actividades_select.click()
                logger.info("✅ Desplegable de actividades abierto")
                
                # Esperar un momento para que se despliegue
                time.sleep(2)
                
                return True
            else:
                return False
            
        except Exception as e:
            logger.error(f"Error abriendo desplegable de actividades: {str(e)}")
            return False
    
    def select_actividad(self, actividad_id):
        """Selecciona una actividad específica del desplegable"""
        try:
            logger.info(f"Seleccionando actividad con ID: {actividad_id}")
            
            # Intentar diferentes estrategias para encontrar el elemento
            actividades_select = None
            
            # Estrategia 1: Buscar por ID
            try:
                actividades_select = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "cmb_actividades"))
                )
                logger.info("✅ Elemento encontrado por ID")
            except:
                logger.warning("No se encontró por ID, intentando por CSS selector...")
                
                # Estrategia 2: Buscar por CSS selector
                try:
                    actividades_select = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "select[name='cmb_actividades']"))
                    )
                    logger.info("✅ Elemento encontrado por CSS selector")
                except:
                    logger.warning("No se encontró por CSS selector, intentando por nombre...")
                    
                    # Estrategia 3: Buscar por nombre
                    try:
                        actividades_select = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.NAME, "cmb_actividades"))
                        )
                        logger.info("✅ Elemento encontrado por nombre")
                    except:
                        logger.error("No se pudo encontrar el elemento del desplegable")
                        return False
            
            if not actividades_select:
                return False
            
            # Importar Select para manejar el dropdown
            from selenium.webdriver.support.ui import Select
            
            # Crear objeto Select y seleccionar la opción
            select = Select(actividades_select)
            select.select_by_value(str(actividad_id))
            
            logger.info(f"✅ Actividad {actividad_id} seleccionada")
            
            # Esperar a que cargue la tabla
            logger.info("Esperando a que cargue la tabla...")
            time.sleep(5)
            
            return True
            
        except Exception as e:
            logger.error(f"Error seleccionando actividad {actividad_id}: {str(e)}")
            return False
    
    def get_table_data(self):
        """Extrae los datos de la tabla de ventas diarias"""
        try:
            logger.info("Extrayendo datos de la tabla...")
            
            # Buscar la tabla
            tabla = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.table.table-striped.table-hover.table-sm"))
            )
            
            # Obtener las filas de la tabla (excluyendo el header)
            filas = tabla.find_elements(By.CSS_SELECTOR, "tbody tr")
            
            datos_tabla = []
            
            for i, fila in enumerate(filas):
                try:
                    # Obtener todas las celdas de la fila
                    celdas = fila.find_elements(By.TAG_NAME, "td")
                    
                    if len(celdas) >= 5:  # Verificar que tenga las 5 columnas esperadas
                        fila_data = {
                            'indice': i + 1,
                            'fecha': celdas[0].text.strip(),
                            'tickets_vendidos': celdas[1].text.strip(),
                            'valor': celdas[2].text.strip(),
                            'tickets_vendidos_acum': celdas[3].text.strip(),
                            'valor_acum': celdas[4].text.strip()
                        }
                        
                        # Verificar si es la fila de TOTAL
                        if 'TOTAL' in fila_data['fecha']:
                            fila_data['es_total'] = True
                        else:
                            fila_data['es_total'] = False
                        
                        datos_tabla.append(fila_data)
                        logger.info(f"Fila {i+1}: {fila_data['fecha']} - {fila_data['tickets_vendidos']} tickets - {fila_data['valor']}")
                    
                except Exception as e:
                    logger.error(f"Error procesando fila {i+1}: {str(e)}")
                    continue
            
            logger.info(f"✅ Extraídos {len(datos_tabla)} registros de la tabla")
            return datos_tabla
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de la tabla: {str(e)}")
            return []
    
    def save_table_data_to_json(self, datos_tabla, actividad_info, filename=None):
        """Guarda los datos de la tabla en un archivo JSON"""
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                actividad_nombre = actividad_info['texto'].replace(' ', '_').replace('/', '_')
                filename = f"ticketshows_tabla_{actividad_nombre}_{timestamp}.json"
            
            filepath = os.path.join(self.download_folder, filename)
            
            data = {
                'fecha_extraccion': datetime.now().isoformat(),
                'actividad': {
                    'id': actividad_info['valor'],
                    'nombre': actividad_info['texto']
                },
                'total_registros': len(datos_tabla),
                'datos_tabla': datos_tabla
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Datos de tabla guardados en: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error guardando datos de tabla: {str(e)}")
            return None
    
    def save_table_data_to_json_with_credential(self, datos_tabla, actividad_info, username, filename=None):
        """Guarda los datos de la tabla en un archivo JSON con información de credencial"""
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                actividad_nombre = actividad_info['texto'].replace(' ', '_').replace('/', '_')
                filename = f"ticketshows_tabla_{actividad_nombre}_{username}_{timestamp}.json"
            
            filepath = os.path.join(self.download_folder, filename)
            
            data = {
                'metadata': {
                    'usuario_utilizado': username,
                    'fecha_extraccion': datetime.now().isoformat(),
                    'timestamp': timestamp
                },
                'actividad': {
                    'id': actividad_info['valor'],
                    'nombre': actividad_info['texto']
                },
                'total_registros': len(datos_tabla),
                'datos_tabla': datos_tabla
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Datos de tabla guardados en: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error guardando datos de tabla: {str(e)}")
            return None
    
    def get_eventos_disponibles(self):
        """Obtiene la lista de eventos disponibles"""
        try:
            logger.info("Obteniendo eventos disponibles...")
            
            # Esperar a que la página cargue después del login
            time.sleep(3)
            
            # Buscar elementos que contengan información de eventos
            # Esto puede variar según la estructura de la página de ticketshows
            eventos = []
            
            # Intentar diferentes selectores para encontrar eventos
            selectors = [
                "div.evento",
                "div.card",
                "div.event",
                "table tbody tr",
                ".evento-item"
            ]
            
            for selector in selectors:
                try:
                    elementos = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elementos:
                        logger.info(f"Encontrados {len(elementos)} elementos con selector: {selector}")
                        break
                except:
                    continue
            
            # Si no se encontraron elementos, registrar la página actual
            if not elementos:
                logger.warning("No se encontraron elementos de eventos")
                # Captura de pantalla comentada
                # self.save_page_screenshot("pagina_despues_login")
                return []
            
            # Procesar los elementos encontrados
            for i, elemento in enumerate(elementos):
                try:
                    # Extraer información básica del evento
                    evento_info = {
                        'indice': i + 1,
                        'texto': elemento.text.strip(),
                        'html': elemento.get_attribute('outerHTML')
                    }
                    
                    # Intentar extraer información específica
                    try:
                        # Buscar título del evento
                        titulo_elem = elemento.find_element(By.CSS_SELECTOR, "h1, h2, h3, h4, h5, .titulo, .title")
                        evento_info['titulo'] = titulo_elem.text.strip()
                    except:
                        evento_info['titulo'] = "Sin título"
                    
                    try:
                        # Buscar fecha del evento
                        fecha_elem = elemento.find_element(By.CSS_SELECTOR, ".fecha, .date, [class*='fecha'], [class*='date']")
                        evento_info['fecha'] = fecha_elem.text.strip()
                    except:
                        evento_info['fecha'] = "Sin fecha"
                    
                    try:
                        # Buscar ubicación del evento
                        ubicacion_elem = elemento.find_element(By.CSS_SELECTOR, ".ubicacion, .location, [class*='ubicacion'], [class*='location']")
                        evento_info['ubicacion'] = ubicacion_elem.text.strip()
                    except:
                        evento_info['ubicacion'] = "Sin ubicación"
                    
                    eventos.append(evento_info)
                    
                except Exception as e:
                    logger.error(f"Error procesando elemento {i+1}: {str(e)}")
                    continue
            
            logger.info(f"Procesados {len(eventos)} eventos")
            return eventos
            
        except Exception as e:
            logger.error(f"Error obteniendo eventos: {str(e)}")
            return []
    
    def save_page_screenshot(self, filename):
        """Guarda una captura de pantalla de la página actual"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename}_{timestamp}.png"
            filepath = os.path.join(self.download_folder, filename)
            self.driver.save_screenshot(filepath)
            logger.info(f"Captura de pantalla guardada: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error guardando captura de pantalla: {str(e)}")
            return None
    
    def save_actividades_to_json(self, actividades, filename=None):
        """Guarda las actividades en un archivo JSON"""
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"ticketshows_actividades_{timestamp}.json"
            
            filepath = os.path.join(self.download_folder, filename)
            
            data = {
                'fecha_extraccion': datetime.now().isoformat(),
                'total_actividades': len(actividades),
                'actividades': actividades
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Datos de actividades guardados en: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error guardando datos de actividades: {str(e)}")
            return None
    
    def save_eventos_to_json(self, eventos, filename=None):
        """Guarda los eventos en un archivo JSON"""
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"ticketshows_data_{timestamp}.json"
            
            filepath = os.path.join(self.download_folder, filename)
            
            data = {
                'fecha_extraccion': datetime.now().isoformat(),
                'total_eventos': len(eventos),
                'eventos': eventos
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Datos guardados en: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error guardando datos: {str(e)}")
            return None

def main():
    """Función principal para ejecutar el scraper con múltiples credenciales"""
    scraper = None
    
    try:
        logger.info("Iniciando scraper de TicketShows con múltiples credenciales...")
        
        # Inicializar scraper
        scraper = TicketShowsScraper(headless=False)  # False para ver el navegador
        
        # Configurar driver
        if not scraper.setup_driver():
            logger.error("No se pudo configurar el driver")
            return
        
        successful_runs = 0
        failed_runs = 0
        
        # Ejecutar el proceso completo con cada credencial
        for i in range(len(scraper.credentials)):
            try:
                if scraper.run_with_credential(i):
                    successful_runs += 1
                    logger.info(f"Credencial {i+1} procesada exitosamente")
                else:
                    failed_runs += 1
                    logger.warning(f"Credencial {i+1} falló")
                
                # Pequeña pausa entre credenciales
                if i < len(scraper.credentials) - 1:  # No hacer pausa después de la última
                    logger.info("Esperando antes de procesar siguiente credencial...")
                    time.sleep(2)
                    
            except Exception as e:
                failed_runs += 1
                logger.error(f"Error procesando credencial {i+1}: {str(e)}")
                continue
        
        # Resumen final
        logger.info(f"=== RESUMEN FINAL ===")
        logger.info(f"Credenciales procesadas exitosamente: {successful_runs}")
        logger.info(f"Credenciales que fallaron: {failed_runs}")
        logger.info(f"Total credenciales: {len(scraper.credentials)}")
        
        return successful_runs > 0

    except Exception as e:
        logger.error(f"Error en función principal: {str(e)}")
        return False
    
    finally:
        if scraper:
            scraper.close()


def main_single_credential():
    """Función original para ejecutar el scraper con una sola credencial (para debugging)"""
    scraper = None
    
    try:
        # Inicializar scraper
        scraper = TicketShowsScraper(headless=False)  # False para ver el navegador
        
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
        username = "dukiec"
        password = "2025"
        
        print(f"\nIntentando login con usuario: {username}")
        
        if scraper.login(username, password):
            print("✅ Login exitoso!")
            
            # Obtener información después del login
            new_page_info = scraper.get_page_info()
            if new_page_info:
                print(f"\nDespués del login:")
                print(f"Título: {new_page_info['title']}")
                print(f"URL: {new_page_info['url']}")
            
            # Captura de pantalla comentada
            # scraper.save_page_screenshot("pagina_despues_login")
            
            # Navegar al menú de Reportes
            print("\n=== NAVEGANDO A REPORTES ===")
            if scraper.click_reportes_menu():
                print("✅ Menú de Reportes abierto")
                
                # Hacer clic en Venta Diaria
                if scraper.click_venta_diaria():
                    print("✅ Venta Diaria seleccionado")
                    
                    # Captura de pantalla comentada
                    # scraper.save_page_screenshot("pagina_venta_diaria")
                    
                    # Debug específico de la página de Venta Diaria
                    print("\n=== DEBUG PÁGINA VENTA DIARIA ===")
                    actividades = scraper.debug_venta_diaria_page()
                    
                    # Obtener información del desplegable de actividades
                    print("\n=== ANALIZANDO DESPLEGABLE DE ACTIVIDADES ===")
                    
                    if actividades:
                        print(f"\n✅ Encontradas {len(actividades)} actividades en el desplegable:")
                        print("\n📋 OPCIONES DISPONIBLES:")
                        for actividad in actividades:
                            if actividad['valor'] != '0':  # Excluir la opción por defecto
                                print(f"  • {actividad['texto']} (ID: {actividad['valor']})")
                            else:
                                print(f"  • {actividad['texto']} (Opción por defecto)")
                        
                        print(f"\n📊 RESUMEN: {len(actividades)-1} actividades disponibles (excluyendo opción por defecto)")
                        
                        # Procesar cada actividad y extraer datos de tabla
                        print("\n=== PROCESANDO DATOS DE TABLAS ===")
                        
                        for actividad in actividades:
                            if actividad['valor'] != '0':  # Excluir la opción por defecto
                                print(f"\n🔄 Procesando: {actividad['texto']} (ID: {actividad['valor']})")
                                
                                # Seleccionar la actividad
                                if scraper.select_actividad(actividad['valor']):
                                    print(f"✅ Actividad seleccionada: {actividad['texto']}")
                                    
                                    # Extraer datos de la tabla
                                    datos_tabla = scraper.get_table_data()
                                    
                                    if datos_tabla:
                                        print(f"✅ Extraídos {len(datos_tabla)} registros de la tabla")
                                        
                                        # Guardar datos en JSON
                                        json_file = scraper.save_table_data_to_json(datos_tabla, actividad)
                                        if json_file:
                                            print(f"✅ Datos guardados en: {json_file}")
                                        else:
                                            print("❌ Error guardando datos de tabla")
                                    else:
                                        print("❌ No se encontraron datos en la tabla")
                                else:
                                    print(f"❌ Error seleccionando actividad: {actividad['texto']}")
                        
                        print(f"\n✅ Procesamiento completado. Se procesaron {len(actividades)-1} actividades.")
                    else:
                        print("❌ No se encontraron actividades en el debug")
                        print("Intentando métodos alternativos...")
                        
                        # Intentar cambiar al contexto principal si estamos en un frame
                        scraper.switch_to_default_content()
                        
                        # Intentar obtener las opciones del select directamente
                        print("\n🔍 BUSCANDO OPCIONES DEL SELECT...")
                        actividades = scraper.get_actividades_dropdown()
                        
                        if actividades:
                            print(f"\n✅ Encontradas {len(actividades)} actividades en el desplegable:")
                            print("\n📋 OPCIONES DISPONIBLES:")
                            for actividad in actividades:
                                if actividad['valor'] != '0':  # Excluir la opción por defecto
                                    print(f"  • {actividad['texto']} (ID: {actividad['valor']})")
                                else:
                                    print(f"  • {actividad['texto']} (Opción por defecto)")
                            
                            print(f"\n📊 RESUMEN: {len(actividades)-1} actividades disponibles (excluyendo opción por defecto)")
                        else:
                            print("❌ No se encontraron opciones del select")
                            print("Intentando abrir el desplegable primero...")
                        
                        # Primero hacer clic en el desplegable para abrirlo
                        if scraper.click_actividades_dropdown():
                            print("✅ Desplegable de actividades abierto")
                            
                            # Ahora obtener las opciones después de abrir el desplegable
                            actividades = scraper.get_actividades_dropdown()
                            
                            if actividades:
                                print(f"\n✅ Encontradas {len(actividades)} actividades en el desplegable:")
                                print("\n📋 OPCIONES DISPONIBLES:")
                                for actividad in actividades:
                                    if actividad['valor'] != '0':  # Excluir la opción por defecto
                                        print(f"  • {actividad['texto']} (ID: {actividad['valor']})")
                                    else:
                                        print(f"  • {actividad['texto']} (Opción por defecto)")
                                
                                print(f"\n📊 RESUMEN: {len(actividades)-1} actividades disponibles (excluyendo opción por defecto)")
                                
                                # Procesar cada actividad y extraer datos de tabla
                                print("\n=== PROCESANDO DATOS DE TABLAS ===")
                                
                                for actividad in actividades:
                                    if actividad['valor'] != '0':  # Excluir la opción por defecto
                                        print(f"\n🔄 Procesando: {actividad['texto']} (ID: {actividad['valor']})")
                                        
                                        # Seleccionar la actividad
                                        if scraper.select_actividad(actividad['valor']):
                                            print(f"✅ Actividad seleccionada: {actividad['texto']}")
                                            
                                            # Extraer datos de la tabla
                                            datos_tabla = scraper.get_table_data()
                                            
                                            if datos_tabla:
                                                print(f"✅ Extraídos {len(datos_tabla)} registros de la tabla")
                                                
                                                # Guardar datos en JSON
                                                json_file = scraper.save_table_data_to_json(datos_tabla, actividad)
                                                if json_file:
                                                    print(f"✅ Datos guardados en: {json_file}")
                                                else:
                                                    print("❌ Error guardando datos de tabla")
                                            else:
                                                print("❌ No se encontraron datos en la tabla")
                                        else:
                                            print(f"❌ Error seleccionando actividad: {actividad['texto']}")
                                
                                print(f"\n✅ Procesamiento completado. Se procesaron {len(actividades)-1} actividades.")
                            else:
                                print("❌ No se encontraron actividades en el desplegable")
                        else:
                            print("❌ Error abriendo el desplegable de actividades")
                else:
                    print("❌ Error navegando a Venta Diaria")
            else:
                print("❌ Error abriendo menú de Reportes")
            
            # Procesar eventos disponibles (comentado por ahora - esperando instrucciones)
            # print("\n=== PROCESANDO EVENTOS DISPONIBLES ===")
            # eventos = scraper.get_eventos_disponibles()
            # 
            # if eventos:
            #     print(f"\nEncontrados {len(eventos)} eventos:")
            #     for evento in eventos:
            #         print(f"- {evento.get('titulo', 'Sin título')} ({evento.get('fecha', 'Sin fecha')})")
            #     
            #     # Guardar eventos en JSON
            #     json_file = scraper.save_eventos_to_json(eventos)
            #     if json_file:
            #         print(f"✅ Datos guardados en: {json_file}")
            #     else:
            #         print("❌ Error guardando datos")
            # else:
            #     print("❌ No se encontraron eventos")
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

if __name__ == "__main__":
    main()
