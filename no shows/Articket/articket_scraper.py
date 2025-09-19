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
import psycopg2
from psycopg2.extras import RealDictCursor
from database_config import get_database_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ArticketScraper:
    def __init__(self, headless=False):
        """
        Inicializa el scraper de Articket optimizado para base de datos
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless
        """
        self.driver = None
        self.headless = headless
        self.base_url = "https://articket.com.ar/auth/login/?next=/administracion/"
        
        # Configuración para contenedores (no crear carpetas físicas)
        self.download_folder = "/tmp"  # Usar /tmp en contenedores
        
        # Inicializar conexión a base de datos
        self.db_connection = None
        self.db_connected = False
        self.setup_database_connection()
        
        # Datos finales para retornar (sin archivos físicos)
        self.final_data = {
            "ticketera": "Articket",
            "fecha_extraccion": None,
            "total_eventos_procesados": 0,
            "eventos_exitosos": 0,
            "eventos_con_error": 0,
            "datos_por_evento": {}
        }
    
    def setup_database_connection(self):
        """Verifica conexión con la base de datos PostgreSQL"""
        try:
            logger.info("🔌 Verificando conexión con la base de datos...")
            connection = get_database_connection()
            if connection:
                cursor = connection.cursor()
                cursor.execute("SELECT NOW();")
                result = cursor.fetchone()
                logger.info(f"✅ Conexión exitosa! Hora actual: {result[0]}")
                cursor.close()
                connection.close()
                self.db_connected = True
                return True
            else:
                logger.warning("⚠️ No se pudo establecer conexión a la base de datos")
                self.db_connected = False
                return False
        except Exception as e:
            logger.error(f"❌ Error en la conexión a la base de datos: {e}")
            self.db_connected = False
            return False
        
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
            username_field.clear()
            username_field.send_keys(username)
            logger.info(f"Usuario ingresado: {username}")
            
            # Buscar y llenar el campo de contraseña
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_password"))
            )
            password_field.clear()
            password_field.send_keys(password)
            logger.info("Contraseña ingresada")
            
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
        """Cierra el driver del navegador"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")
    
    def get_eventos_en_curso(self):
        """Obtiene la lista de eventos en curso desde la tabla"""
        try:
            logger.info("Obteniendo eventos en curso...")
            
            # Esperar a que la tabla cargue
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "eventos_en_curso"))
            )
            
            # Buscar todas las filas de la tabla
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#eventos_en_curso tbody tr")
            logger.info(f"Encontrados {len(rows)} eventos en curso")
            
            # Verificar si no hay filas o si las filas están vacías
            if len(rows) == 0:
                logger.info("No hay filas en la tabla de eventos en curso")
                return []
            
            eventos = []
            for i, row in enumerate(rows):
                try:
                    # Verificar si la fila tiene contenido válido
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) == 0:
                        logger.info(f"Fila {i+1} no tiene celdas, omitiendo...")
                        continue
                    
                    # Verificar si la primera celda contiene un enlace
                    evento_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                    links = evento_cell.find_elements(By.TAG_NAME, "a")
                    if len(links) == 0:
                        logger.info(f"Fila {i+1} no contiene enlace de evento, omitiendo...")
                        continue
                    
                    evento_nombre = links[0].text.strip()
                    
                    # Si el nombre está vacío, omitir esta fila
                    if not evento_nombre:
                        logger.info(f"Fila {i+1} tiene enlace pero sin texto, omitiendo...")
                        continue
                    
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
                    
                    # Buscar el botón de borderaux (ícono de silla)
                    try:
                        borderaux_link = row.find_element(By.CSS_SELECTOR, "a.btn-circle.btn-outline-primary i.fa.fa-chair").find_element(By.XPATH, "./..")
                        borderaux_url = borderaux_link.get_attribute("href")
                    except:
                        logger.warning(f"No se encontró botón borderaux para evento: {evento_nombre}")
                        continue
                    
                    evento_info = {
                        'indice': i + 1,
                        'nombre': evento_nombre,
                        'fecha': fecha_evento,
                        'recinto': recinto,
                        'ciudad': ciudad,
                        'borderaux_url': borderaux_url
                    }
                    
                    eventos.append(evento_info)
                    logger.info(f"Evento {i+1}: {evento_nombre} - {fecha_evento} - {ciudad}")
                    
                except Exception as e:
                    logger.warning(f"Error procesando fila {i+1}: {str(e)}")
                    continue
            
            # Si no se encontraron eventos válidos después de procesar todas las filas
            if len(eventos) == 0:
                logger.info("No se encontraron eventos válidos en la tabla")
            
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
            
            # Guardar en base de datos en lugar de archivo JSON
            success = self.save_to_database(evento_info, info_borderaux)
            
            if success:
                logger.info(f"✅ Información de borderaux guardada en base de datos para: {evento_info['nombre']}")
                return True
            else:
                logger.error(f"❌ Error guardando en base de datos para: {evento_info['nombre']}")
                return False
            
        except Exception as e:
            logger.error(f"Error extrayendo información de borderaux: {str(e)}")
            return False
    
    def save_to_database(self, evento_info, json_data):
        """
        Guarda los datos del evento en la tabla raw_data de la base de datos
        
        Args:
            evento_info (dict): Información del evento
            json_data (dict): Datos JSON del evento
        """
        try:
            if not self.db_connected:
                logger.warning("⚠️ Base de datos no conectada, no se pueden guardar datos")
                return False
            
            logger.info(f"💾 Guardando datos de '{evento_info['nombre']}' en la base de datos...")
            
            # Obtener conexión
            connection = get_database_connection()
            if not connection:
                logger.error("❌ No se pudo obtener conexión a la base de datos")
                return False
            
            cursor = connection.cursor()
            
            # Preparar datos para inserción
            # Restar 3 horas directamente para hora de Argentina (UTC-3)
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            
            # Parsear fecha del evento
            try:
                fecha_str = evento_info['fecha']
                if "/" in fecha_str:
                    # Formato: 15/10/2025
                    fecha_parts = fecha_str.split("/")
                    if len(fecha_parts) == 3:
                        fecha_evento_parsed = f"{fecha_parts[2]}-{fecha_parts[1]}-{fecha_parts[0]}"
                    else:
                        logger.warning(f"⚠️ Formato de fecha inválido: {fecha_str}")
                        fecha_evento_parsed = fecha_str
                else:
                    fecha_evento_parsed = fecha_str
            except Exception as e:
                logger.warning(f"⚠️ No se pudo parsear fecha del evento: {e}")
                fecha_evento_parsed = evento_info['fecha']
            
            # Calcular totales para este evento
            totales_evento = self.calculate_event_totals(json_data)
            
            # Crear JSON individual para este evento con totales calculados
            json_individual = {
                'evento': evento_info['nombre'],
                'venue': evento_info['recinto'],
                'fecha_evento': evento_info['fecha'],
                'ciudad': evento_info['ciudad'],
                'totales_evento': totales_evento,
                'categorias': json_data.get('categorias', []),
                'resumen_total': json_data.get('resumen_total', {})
            }
            
            # Preparar datos para inserción
            insert_data = {
                "ticketera": "Articket",
                "artista": evento_info['nombre'],  # En Articket, el artista es el nombre del evento
                "venue": evento_info['recinto'],
                "fecha_show": fecha_evento_parsed,
                "json_data": json.dumps(json_individual, ensure_ascii=False),
                "archivo_origen": f"articket_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "url_origen": self.base_url,
                "fecha_extraccion": fecha_extraccion_utc3.isoformat(),
                "procesado": False
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
                logger.info(f"✅ Datos de '{evento_info['nombre']}' guardados exitosamente en la BD (ID: {result[0]})")
                print(f"💾 GUARDADO EN BD: {evento_info['nombre']} - {evento_info['fecha']} - ID: {result[0]}")
                
                # Commit y cerrar conexión
                connection.commit()
                cursor.close()
                connection.close()
                
                return True
            else:
                logger.warning(f"⚠️ Inserción completada pero sin ID retornado para '{evento_info['nombre']}'")
                return False
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos de '{evento_info['nombre']}' en la BD: {str(e)}")
            print(f"❌ ERROR GUARDANDO EN BD: {evento_info['nombre']} - {str(e)}")
            
            # Intentar cerrar conexión si está abierta
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'connection' in locals() and connection:
                    connection.rollback()
                    connection.close()
            except:
                pass
            
            return False
    
    def calculate_event_totals(self, json_data):
        """
        Calcula los totales de un evento específico basándose en los datos extraídos
        
        Args:
            json_data (dict): Datos JSON del evento
            
        Returns:
            dict: Diccionario con todos los totales calculados
        """
        try:
            # Obtener totales del resumen
            resumen_total = json_data.get("resumen_total", {})
            
            # Calcular totales del evento
            totales_evento = {
                "capacidad_total": resumen_total.get("total_cupos", 0),
                "vendido_total": resumen_total.get("total_cantidad_vendida", 0),
                "disponible_total": resumen_total.get("total_cupos_libres", 0),
                "recaudacion_total_ars": resumen_total.get("total_general", 0),
                "porcentaje_ocupacion": 0
            }
            
            # Calcular porcentaje de ocupación
            if totales_evento["capacidad_total"] > 0:
                totales_evento["porcentaje_ocupacion"] = round(
                    (totales_evento["vendido_total"] / totales_evento["capacidad_total"]) * 100, 2
                )
            
            logger.info(f"📊 Totales calculados para evento:")
            logger.info(f"  📊 Capacidad: {totales_evento['capacidad_total']}")
            logger.info(f"  🎫 Vendido: {totales_evento['vendido_total']}")
            logger.info(f"  🆓 Disponible: {totales_evento['disponible_total']}")
            logger.info(f"  💰 Recaudación: ${totales_evento['recaudacion_total_ars']:,}")
            logger.info(f"  📈 Ocupación: {totales_evento['porcentaje_ocupacion']}%")
            
            return totales_evento
            
        except Exception as e:
            logger.error(f"❌ Error calculando totales del evento: {str(e)}")
            return {
                "capacidad_total": 0,
                "vendido_total": 0,
                "disponible_total": 0,
                "recaudacion_total_ars": 0,
                "porcentaje_ocupacion": 0
            }
    
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
            self.driver.get("https://articket.com.ar/administracion/")
            
            # Esperar a que cargue la página
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "eventos_en_curso"))
            )
            
            logger.info("✅ Vuelto exitosamente a la página de eventos")
            return True
            
        except Exception as e:
            logger.error(f"Error volviendo a eventos: {str(e)}")
            return False

def main():
    """Función principal para ejecutar el scraper"""
    scraper = None
    
    try:
        # Inicializar scraper
        scraper = ArticketScraper(headless=True)  # True para modo headless
        
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
        username = "Daleplay"
        password = "DP24"
        
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
                print("❌ No hay eventos en proceso")
                print("✅ Proceso completado - No hay datos para extraer")
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

def run_scraper_for_airflow():
    """
    Función específica para ejecutar desde Airflow
    Retorna los datos extraídos en formato JSON para enviar a base de datos
    
    Returns:
        dict: Datos completos extraídos o None si hay error
    """
    try:
        logger.info("🚀 INICIANDO SCRAPER ARTICKET PARA AIRFLOW")
        result = main()
        return result
    except Exception as e:
        logger.error(f"❌ Error ejecutando scraper para Airflow: {str(e)}")
        return None

if __name__ == "__main__":
    main()
