from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
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

class PuntoTicketScraper:
    def __init__(self, headless=True):  # Por defecto headless para contenedores
        """
        Inicializa el scraper de Punto Ticket optimizado para Airflow
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless (recomendado para contenedores)
        """
        self.driver = None
        self.headless = headless
        self.download_folder = "/tmp"  # Usar /tmp en contenedores
        
        # Credenciales de login
        self.username = "mgiglio"
        self.password = "FMJBBWT"
        self.login_url = "https://backoffice.puntoticket.com/Report?title=1.%20Resumen%20Ventas&reporteId=4&eventoId=BIZ183"
        
        # Datos finales para retornar (sin archivos físicos)
        self.final_data = {
            "ticketera": "Punto Ticket",
            "fecha_extraccion": None,
            "total_eventos_procesados": 0,
            "eventos_exitosos": 0,
            "eventos_con_error": 0,
            "datos_por_evento": {}
        }
        
        logger.info("=== INICIALIZACIÓN DEL SCRAPER PUNTO TICKET PARA AIRFLOW ===")
        logger.info(f"URL objetivo: {self.login_url}")
        logger.info(f"Modo headless: {self.headless}")
        logger.info(f"Modo contenedor: Sin archivos físicos")
        
        # Inicializar conexión a base de datos
        self.db_connection = None
        self.setup_database_connection()
    
    def setup_database_connection(self):
        """Verifica conexión con la base de datos PostgreSQL"""
        try:
            logger.info("🔌 Verificando conexión con la base de datos...")
            self.db_connection = get_database_connection()
            if self.db_connection:
                logger.info("✅ Conexión a base de datos establecida")
                return True
            else:
                logger.error("❌ No se pudo establecer conexión a la base de datos")
                return False
        except Exception as e:
            logger.error(f"❌ Error conectando a la base de datos: {str(e)}")
            return False
    
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
    
    def login(self):
        """Realiza el login en el sistema Punto Ticket"""
        try:
            logger.info("Iniciando proceso de login...")
            
            # Navegar a la página de login
            logger.info(f"Navegando a: {self.login_url}")
            self.driver.get(self.login_url)
            time.sleep(3)
            
            # Buscar el formulario de login
            try:
                login_form = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "logon-form"))
                )
                logger.info("Formulario de login encontrado")
            except:
                logger.error("No se encontró el formulario de login")
                return False
            
            # Ingresar usuario
            try:
                user_field = self.driver.find_element(By.ID, "UserName")
                user_field.clear()
                user_field.send_keys(self.username)
                logger.info(f"Usuario ingresado: {self.username}")
            except:
                logger.error("No se encontró el campo de usuario")
                return False
            
            # Ingresar contraseña
            try:
                password_field = self.driver.find_element(By.ID, "Password")
                password_field.clear()
                password_field.send_keys(self.password)
                logger.info("Contraseña ingresada")
            except:
                logger.error("No se encontró el campo de contraseña")
                return False
            
            # Hacer clic en el botón "Ingresar"
            try:
                submit_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
                submit_button.click()
                logger.info("Botón 'Ingresar' clickeado")
                time.sleep(5)
            except:
                logger.error("No se encontró el botón 'Ingresar'")
                return False
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            logger.info(f"URL actual después del login: {current_url}")
            
            # Si ya no estamos en una página de login, el login fue exitoso
            if "logon" not in current_url.lower() and "login" not in current_url.lower():
                logger.info("Login exitoso")
                return True
            else:
                # Verificar si hay mensajes de error
                try:
                    error_element = self.driver.find_element(By.CLASS_NAME, "validation-summary-valid")
                    if error_element.is_displayed():
                        logger.error("Error de login detectado")
                        return False
                except:
                    pass
                
                logger.warning("El login puede no haber sido exitoso")
                return False
                
        except Exception as e:
            logger.error(f"Error durante el login: {str(e)}")
            return False
    
    def get_all_events(self):
        """Obtiene todos los eventos disponibles del dropdown"""
        try:
            logger.info("Obteniendo lista de eventos...")
            
            # Buscar el select de eventos
            try:
                event_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "eventoId"))
                )
                logger.info("Dropdown de eventos encontrado")
            except:
                logger.error("No se encontró el dropdown de eventos")
                return []
            
            # Obtener todas las opciones
            select = Select(event_select)
            options = select.options
            
            events = []
            for option in options:
                event_value = option.get_attribute("value")
                event_text = option.text.strip()
                
                # Saltar la opción vacía
                if event_value and event_text:
                    events.append({
                        "value": event_value,
                        "text": event_text
                    })
            
            logger.info(f"Encontrados {len(events)} eventos")
            return events
            
        except Exception as e:
            logger.error(f"Error obteniendo eventos: {str(e)}")
            return []
    
    def select_event_and_get_report(self, event_value, event_text):
        """Selecciona un evento y obtiene el reporte"""
        try:
            logger.info(f"Procesando evento: {event_text}")
            
            # Seleccionar el evento
            try:
                event_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "eventoId"))
                )
                select = Select(event_select)
                select.select_by_value(event_value)
                logger.info(f"Evento seleccionado: {event_value}")
                time.sleep(2)
            except:
                logger.error(f"No se pudo seleccionar el evento {event_value}")
                return None
            
            # Hacer clic en "Ver reporte"
            try:
                report_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[ng-click='vm.onBtnGetData()']"))
                )
                report_button.click()
                logger.info("Botón 'Ver reporte' clickeado")
                time.sleep(5)
            except:
                logger.error("No se encontró el botón 'Ver reporte'")
                return None
            
            # Esperar a que se carguen las tablas
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-stats"))
                )
                logger.info("Tablas de reporte cargadas")
            except:
                logger.warning("No se encontraron tablas de reporte para este evento")
                return None
            
            # Extraer datos de las tablas
            return self.extract_table_data(event_value, event_text)
            
        except Exception as e:
            logger.error(f"Error procesando evento {event_text}: {str(e)}")
            return None
    
    def extract_table_data(self, event_value, event_text):
        """Extrae datos de las tablas de cantidad y monto de ventas"""
        try:
            logger.info("Extrayendo datos de las tablas...")
            
            # Buscar todas las tablas
            tables = self.driver.find_elements(By.CSS_SELECTOR, "table.table-stats")
            
            if len(tables) < 1:
                logger.warning(f"No se encontraron tablas de reporte")
                return None
            
            logger.info(f"Encontradas {len(tables)} tablas")
            
            extracted_data = {
                "evento_codigo": event_value,
                "evento_nombre": event_text,
                "fecha_extraccion": datetime.now().isoformat(),
                "cantidad_ventas": [],
                "monto_ventas": []
            }
            
            # Procesar todas las tablas encontradas
            for i, table in enumerate(tables):
                table_type = "Cantidad Ventas" if i == 0 else f"Monto Ventas" if i == 1 else f"Tabla {i+1}"
                logger.info(f"Procesando tabla {i+1}: {table_type}")
                
                table_data = self.process_table(table, table_type)
                if table_data:
                    if i == 0:
                        extracted_data["cantidad_ventas"] = table_data
                    elif i == 1:
                        extracted_data["monto_ventas"] = table_data
                    else:
                        # Para tablas adicionales
                        extracted_data[f"tabla_{i+1}"] = table_data
            
            # Log de debug para ver qué datos tenemos
            logger.info(f"Datos extraídos antes del filtro:")
            logger.info(f"- Cantidad ventas: {len(extracted_data.get('cantidad_ventas', []))} filas")
            logger.info(f"- Monto ventas: {len(extracted_data.get('monto_ventas', []))} filas")
            
            # Mostrar algunas fechas para debug
            for row in extracted_data.get("cantidad_ventas", [])[:3]:
                logger.info(f"Muestra fecha cantidad: {row.get('fecha_evento', 'N/A')}")
            
            # Filtrar por fechas futuras o actuales
            filtered_data = self.filter_future_events(extracted_data)
            
            if not filtered_data:
                logger.info(f"Evento {event_text} no tiene fechas futuras o actuales, se omite")
                return None
            
            logger.info(f"Datos extraídos para evento: {event_text}")
            return filtered_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de tablas: {str(e)}")
            return None
    
    def process_table(self, table, table_type):
        """Procesa una tabla específica y extrae los datos"""
        try:
            # Extraer headers
            headers = []
            try:
                thead = table.find_element(By.TAG_NAME, "thead")
                header_row = thead.find_element(By.TAG_NAME, "tr")
                header_cells = header_row.find_elements(By.TAG_NAME, "th")
                headers = [cell.text.strip() for cell in header_cells]
            except:
                logger.warning(f"No se encontraron headers para tabla {table_type}")
                return []
            
            # Extraer filas de datos
            rows_data = []
            try:
                tbody = table.find_element(By.TAG_NAME, "tbody")
                data_rows = tbody.find_elements(By.TAG_NAME, "tr")
                
                for row in data_rows:
                    cells = row.find_elements(By.CSS_SELECTOR, "td")
                    
                    if cells and len(cells) > 0:
                        row_data = {
                            "tipo_tabla": table_type,
                            "fecha_extraccion": datetime.now().isoformat()
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
                        
                        rows_data.append(row_data)
            
            except Exception as e:
                logger.warning(f"Error procesando filas de tabla {table_type}: {str(e)}")
            
            return rows_data
            
        except Exception as e:
            logger.error(f"Error procesando tabla {table_type}: {str(e)}")
            return []
    
    def filter_future_events(self, event_data):
        """Filtra eventos que tengan fechas actuales o futuras"""
        try:
            today = datetime.now().date()
            has_future_events = False
            
            logger.info(f"Filtrando fechas. Hoy es: {today}")
            
            # Verificar si hay fechas futuras en cantidad_ventas
            filtered_cantidad = []
            for row in event_data.get("cantidad_ventas", []):
                fecha_evento_str = row.get("fecha_evento", "")
                fecha_evento = self.parse_event_date(fecha_evento_str)
                
                if fecha_evento:
                    logger.info(f"Fecha encontrada: {fecha_evento_str} -> {fecha_evento} (¿Futura? {fecha_evento >= today})")
                    if fecha_evento >= today:
                        filtered_cantidad.append(row)
                        has_future_events = True
                else:
                    logger.debug(f"Fecha no válida o total: {fecha_evento_str}")
            
            # Verificar si hay fechas futuras en monto_ventas
            filtered_monto = []
            for row in event_data.get("monto_ventas", []):
                fecha_evento_str = row.get("fecha_evento", "")
                fecha_evento = self.parse_event_date(fecha_evento_str)
                
                if fecha_evento:
                    if fecha_evento >= today:
                        filtered_monto.append(row)
                        has_future_events = True
            
            if not has_future_events:
                logger.info(f"No se encontraron fechas futuras para el evento {event_data.get('evento_nombre', 'N/A')}")
                return None
            
            # Retornar datos filtrados
            filtered_data = event_data.copy()
            filtered_data["cantidad_ventas"] = filtered_cantidad
            filtered_data["monto_ventas"] = filtered_monto
            
            logger.info(f"Evento filtrado: {len(filtered_cantidad)} filas de cantidad, {len(filtered_monto)} filas de monto")
            return filtered_data
            
        except Exception as e:
            logger.error(f"Error filtrando eventos futuros: {str(e)}")
            return event_data  # Retornar datos originales si hay error
    
    def parse_event_date(self, date_string):
        """Parsea la fecha del evento del formato encontrado en las tablas"""
        try:
            if not date_string:
                return None
            
            # Ignorar filas de totales
            if "total" in date_string.lower() or date_string.strip().lower() == "total":
                return None
            
            # Formato esperado: "2025-09-05 21:00"
            date_part = date_string.split(" ")[0]  # Obtener solo la parte de la fecha
            
            # Validar que sea una fecha válida (formato YYYY-MM-DD)
            if len(date_part) == 10 and date_part.count("-") == 2:
                event_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                return event_date
            else:
                return None
            
        except Exception as e:
            logger.debug(f"No se pudo parsear la fecha: {date_string} - {str(e)}")
            return None
    
    def calculate_event_totals(self, event_data):
        """
        Calcula los totales de un evento específico basándose en los datos extraídos
        
        Args:
            event_data (dict): Datos del evento
            
        Returns:
            dict: Diccionario con todos los totales calculados
        """
        try:
            logger.info("🧮 Calculando totales del evento...")
            
            # Inicializar totales
            totals = {
                "total_capacidad": 0,
                "total_vendido": 0,
                "total_disponible": 0,
                "total_recaudado": 0,
                "total_eventos": 0
            }
            
            # Procesar datos de cantidad de ventas
            for row in event_data.get("cantidad_ventas", []):
                # Buscar la columna "total" que contiene el total de tickets vendidos
                if "total" in row:
                    try:
                        # Extraer número del valor (remover caracteres no numéricos)
                        value_str = str(row["total"]).strip()
                        numeric_str = re.sub(r'[^\d.,]', '', value_str)
                        
                        if numeric_str:
                            # Manejar formato chileno: 1.017 (puntos como separadores de miles)
                            if '.' in numeric_str and ',' not in numeric_str:
                                # Formato chileno: 1.017 -> 1017
                                numeric_str = numeric_str.replace('.', '')
                            elif ',' in numeric_str and '.' in numeric_str:
                                # Formato mixto: 1,017.50 -> 1017.50
                                numeric_str = numeric_str.replace(',', '').replace('.', '')
                            elif ',' in numeric_str:
                                # Formato con coma decimal: 1017,50 -> 1017.50
                                numeric_str = numeric_str.replace(',', '.')
                            
                            totals["total_vendido"] = int(float(numeric_str))
                            logger.info(f"📊 Total vendido extraído: {totals['total_vendido']}")
                            
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error procesando total vendido: {e}")
                        continue
                
                # Sumar todos los sectores para obtener capacidad total
                for key, value in row.items():
                    if key not in ["tipo_tabla", "fecha_extraccion", "fecha_evento", "evento", "total"]:
                        try:
                            value_str = str(value).strip()
                            numeric_str = re.sub(r'[^\d.,]', '', value_str)
                            
                            if numeric_str:
                                # Manejar formato chileno: 1.017 (puntos como separadores de miles)
                                if '.' in numeric_str and ',' not in numeric_str:
                                    # Formato chileno: 1.017 -> 1017
                                    numeric_str = numeric_str.replace('.', '')
                                elif ',' in numeric_str and '.' in numeric_str:
                                    # Formato mixto: 1,017.50 -> 1017.50
                                    numeric_str = numeric_str.replace(',', '').replace('.', '')
                                elif ',' in numeric_str:
                                    # Formato con coma decimal: 1017,50 -> 1017.50
                                    numeric_str = numeric_str.replace(',', '.')
                                
                                value_num = int(float(numeric_str))
                                totals["total_capacidad"] += value_num
                                    
                        except (ValueError, TypeError):
                            continue
            
            # Procesar datos de monto de ventas
            for row in event_data.get("monto_ventas", []):
                # Buscar la columna "total" que contiene el total recaudado
                if "total" in row:
                    try:
                        value_str = str(row["total"]).strip()
                        numeric_str = re.sub(r'[^\d.,]', '', value_str)
                        
                        if numeric_str:
                            # Manejar formato chileno: 255.620.000 (puntos como separadores de miles)
                            if '.' in numeric_str and ',' not in numeric_str:
                                # Formato chileno: 255.620.000 -> 255620000
                                numeric_str = numeric_str.replace('.', '')
                            elif ',' in numeric_str and '.' in numeric_str:
                                # Formato mixto: 255,620.000 -> 255620000
                                numeric_str = numeric_str.replace(',', '').replace('.', '')
                            elif ',' in numeric_str:
                                # Formato con coma decimal: 255620,50 -> 255620.50
                                numeric_str = numeric_str.replace(',', '.')
                            
                            totals["total_recaudado"] = float(numeric_str)
                            logger.info(f"💰 Total recaudado extraído: ${totals['total_recaudado']:,.2f}")
                            
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error procesando total recaudado: {e}")
                        continue
            
            # Contar eventos únicos
            fechas_unicas = set()
            for row in event_data.get("cantidad_ventas", []):
                fecha = row.get("fecha_evento", "")
                if fecha and fecha.strip():
                    fechas_unicas.add(fecha.strip())
            totals["total_eventos"] = len(fechas_unicas)
            
            # Calcular disponibles (capacidad - vendido)
            totals["total_disponible"] = totals["total_capacidad"] - totals["total_vendido"]
            
            # Calcular porcentaje de ocupación
            if totals["total_capacidad"] > 0:
                totals["porcentaje_ocupacion"] = round(
                    (totals["total_vendido"] / totals["total_capacidad"]) * 100, 2
                )
            else:
                totals["porcentaje_ocupacion"] = 0
            
            logger.info(f"📊 Totales calculados para evento:")
            logger.info(f"  📊 Capacidad: {totals['total_capacidad']}")
            logger.info(f"  🎫 Vendido: {totals['total_vendido']}")
            logger.info(f"  🆓 Disponible: {totals['total_disponible']}")
            logger.info(f"  💰 Recaudado: ${totals['total_recaudado']:,.2f}")
            logger.info(f"  📈 Ocupación: {totals['porcentaje_ocupacion']}%")
            logger.info(f"  🎭 Eventos: {totals['total_eventos']}")
            
            return totals
            
        except Exception as e:
            logger.error(f"❌ Error calculando totales del evento: {str(e)}")
            return {
                "total_capacidad": 0,
                "total_vendido": 0,
                "total_disponible": 0,
                "total_recaudado": 0,
                "total_eventos": 0,
                "porcentaje_ocupacion": 0
            }
    
    def extract_artist_name(self, evento_nombre):
        """
        Extrae solo el nombre del artista del nombre completo del evento
        
        Args:
            evento_nombre (str): Nombre completo del evento (ej: "BIZ328 - Diego Torres - Mejor que Ayer Tour - Movistar Arena")
            
        Returns:
            str: Solo el nombre del artista (ej: "Diego Torres")
        """
        try:
            if not evento_nombre:
                return ""
            
            # Dividir por " - " y tomar la segunda parte (índice 1)
            parts = evento_nombre.split(" - ")
            if len(parts) >= 2:
                return parts[1].strip()
            else:
                # Si no hay separadores, devolver el nombre completo
                return evento_nombre.strip()
                
        except Exception as e:
            logger.error(f"Error extrayendo nombre del artista: {e}")
            return evento_nombre

    def extract_venue_name(self, evento_nombre):
        """
        Extrae el nombre del venue del nombre completo del evento
        
        Args:
            evento_nombre (str): Nombre completo del evento (ej: "BIZ278 - Airbag - Teatro Caupolicán")
            
        Returns:
            str: Solo el nombre del venue (ej: "Teatro Caupolicán")
        """
        try:
            if not evento_nombre:
                return "Movistar Arena"  # Default
            
            # Dividir por " - " y tomar la última parte (índice -1)
            parts = evento_nombre.split(" - ")
            if len(parts) >= 2:
                venue = parts[-1].strip()  # Última parte
                logger.info(f"🏟️ Venue extraído: '{venue}'")
                return venue
            else:
                # Si no hay separadores, usar default
                return "Movistar Arena"
                
        except Exception as e:
            logger.error(f"Error extrayendo nombre del venue: {e}")
            return "Movistar Arena"  # Default en caso de error

    def save_to_database(self, event_data):
        """
        Guarda los datos del evento en la tabla raw_data de la base de datos
        
        Args:
            event_data (dict): Datos del evento extraído
            
        Returns:
            bool: True si se guardó exitosamente, False en caso contrario
        """
        try:
            if not self.db_connection:
                logger.error("❌ No hay conexión a la base de datos")
                return False
            
            # Calcular totales del evento
            totales_evento = self.calculate_event_totals(event_data)
            
            # Preparar datos para inserción
            # Restar 3 horas directamente para hora de Argentina (UTC-3)
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            
            # DEBUG: Mostrar las fechas para verificar
            logger.info(f"🕐 Fecha UTC: {datetime.now()}")
            logger.info(f"🕐 Fecha Argentina (UTC-3): {fecha_extraccion_utc3}")
            logger.info(f"🕐 Fecha ISO Argentina: {fecha_extraccion_utc3.isoformat()}")
            
            # Extraer solo el nombre del artista
            artista_nombre = self.extract_artist_name(event_data.get('evento_nombre', ''))
            logger.info(f"🎤 Artista extraído: '{artista_nombre}'")
            
            # Extraer el nombre del venue
            venue_nombre = self.extract_venue_name(event_data.get('evento_nombre', ''))
            
            # Crear JSON individual para este evento con totales calculados
            json_individual = {
                'evento_codigo': event_data.get('evento_codigo', ''),
                'evento_nombre': event_data.get('evento_nombre', ''),
                'venue': venue_nombre,  # Venue extraído dinámicamente
                'ciudad': 'Santiago de Chile',
                'fecha_extraccion': fecha_extraccion_utc3.isoformat(),
                'totales_evento': totales_evento,  # Totales calculados
                'cantidad_ventas': event_data.get('cantidad_ventas', []),
                'monto_ventas': event_data.get('monto_ventas', [])
            }
            
            # Extraer fecha del evento de los datos
            fecha_evento = None
            for row in event_data.get("cantidad_ventas", []):
                fecha_str = row.get("fecha_evento", "")
                if fecha_str and fecha_str.strip() and fecha_str.strip().lower() != "total":
                    try:
                        # Formato: "2025-11-29 21:00"
                        fecha_evento = datetime.strptime(fecha_str.strip(), "%Y-%m-%d %H:%M")
                        break
                    except ValueError:
                        continue
            
            # Preparar datos para inserción
            insert_data = {
                "ticketera": "Punto Ticket",
                "artista": artista_nombre,  # Solo el nombre del artista
                "venue": venue_nombre,  # Venue extraído dinámicamente
                "fecha_show": fecha_evento,  # Fecha del evento extraída
                "json_data": json.dumps(json_individual, ensure_ascii=False),
                "archivo_origen": f"puntoticket_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "url_origen": self.login_url,
                "fecha_extraccion": fecha_extraccion_utc3.isoformat(),
                "procesado": False
            }
            
            # Insertar en la base de datos
            cursor = self.db_connection.cursor()
            
            insert_query = """
                INSERT INTO raw_data (
                    ticketera, artista, venue, fecha_show, json_data, 
                    archivo_origen, url_origen, fecha_extraccion, procesado
                ) VALUES (
                    %(ticketera)s, %(artista)s, %(venue)s, %(fecha_show)s, %(json_data)s,
                    %(archivo_origen)s, %(url_origen)s, %(fecha_extraccion)s, %(procesado)s
                ) RETURNING id;
                """
            
            cursor.execute(insert_query, insert_data)
            inserted_id = cursor.fetchone()[0]
            self.db_connection.commit()
            cursor.close()
            
            logger.info(f"✅ Datos guardados en raw_data con ID: {inserted_id}")
            logger.info(f"✅ Evento: {event_data.get('evento_nombre', 'N/A')}")
            logger.info(f"✅ Totales: Vendido={totales_evento['total_vendido']}, Recaudado=${totales_evento['total_recaudado']:,.2f}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando en base de datos: {str(e)}")
            if self.db_connection:
                self.db_connection.rollback()
            return False
    
    # def save_data_to_json(self, all_data, filename_prefix="puntoticket_data"):
    #     """Guarda los datos extraídos en un archivo JSON - DESHABILITADO"""
    #     # Función deshabilitada - solo se guardan datos en base de datos
    #     logger.info("⚠️ Generación de archivos JSON deshabilitada - solo BD")
    #     return None
    
    def close_driver(self):
        """Cierra el driver del navegador"""
        try:
            if self.driver:
                self.driver.quit()
                logger.info("Driver cerrado")
        except Exception as e:
            logger.error(f"Error cerrando el driver: {str(e)}")
    
    def run(self):
        """Ejecuta el scraper completo"""
        try:
            logger.info("=== INICIANDO SCRAPER DE PUNTO TICKET ===")
            
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
            
            # Obtener lista de eventos
            logger.info("PASO 3: Obteniendo lista de eventos...")
            events = self.get_all_events()
            
            if not events:
                logger.error("No se pudieron obtener los eventos")
                return False
            
            # Procesar cada evento
            logger.info(f"PASO 4: Procesando {len(events)} eventos...")
            all_extracted_data = []
            successful_events = 0
            failed_events = 0
            
            for i, event in enumerate(events):
                logger.info(f"Procesando evento {i+1}/{len(events)}: {event['text']}")
                
                event_data = self.select_event_and_get_report(event['value'], event['text'])
                
                if event_data:
                    all_extracted_data.append(event_data)
                    
                    # Guardar en base de datos
                    if self.save_to_database(event_data):
                        successful_events += 1
                        logger.info(f"✅ Datos extraídos y guardados para: {event['text']}")
                    else:
                        failed_events += 1
                        logger.error(f"❌ Error guardando en BD para: {event['text']}")
                else:
                    logger.info(f"⏭️ Sin datos futuros para: {event['text']}")
                
                # Pequeña pausa entre eventos
                time.sleep(2)
            
            # Actualizar datos finales
            self.final_data["total_eventos_procesados"] = len(events)
            self.final_data["eventos_exitosos"] = successful_events
            self.final_data["eventos_con_error"] = failed_events
            # Restar 3 horas directamente para hora de Argentina (UTC-3)
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            self.final_data["fecha_extraccion"] = fecha_extraccion_utc3.isoformat()
            
            # Datos guardados solo en base de datos (sin archivos JSON)
            logger.info("=== SCRAPER EJECUTADO EXITOSAMENTE ===")
            logger.info(f"Total de eventos procesados: {len(events)}")
            logger.info(f"Eventos exitosos: {successful_events}")
            logger.info(f"Eventos con error: {failed_events}")
            logger.info("✅ Datos guardados únicamente en base de datos")
            return self.final_data
                
        except Exception as e:
            logger.error(f"Error general en el scraper: {str(e)}")
            return False
        finally:
            # Cerrar navegador automáticamente
            logger.info("Cerrando navegador automáticamente...")
            self.close_driver()

def main():
    """Función principal para testing local"""
    logger.info("=== PUNTO TICKET SCRAPER AUTOMÁTICO ===")
    logger.info("Usuario: mgiglio")
    logger.info("Contraseña: ****")
    logger.info("URL de login: https://backoffice.puntoticket.com/Report...")
    logger.info("🎭 Solo eventos con fechas actuales o futuras")
    
    # Crear y ejecutar el scraper automáticamente
    scraper = PuntoTicketScraper(headless=True)  # Modo headless por defecto
    result = scraper.run()
    
    if result:
        print("✅ Scraper ejecutado exitosamente")
        print(f"📊 Resultados: {result}")
    else:
        print("❌ Error en la ejecución del scraper")
    
    return result

def run_scraper_for_airflow():
    """
    Función específica para ejecutar desde Airflow
    Retorna los datos extraídos en formato JSON para enviar a base de datos
    
    Returns:
        dict: Datos completos extraídos o None si hay error
    """
    try:
        logger.info("🚀 INICIANDO SCRAPER PUNTO TICKET PARA AIRFLOW")
        result = main()
        return result
    except Exception as e:
        logger.error(f"❌ Error ejecutando scraper para Airflow: {str(e)}")
        return None

def test_local():
    """Función para testing local con Chrome visible"""
    logger.info("=== PUNTO TICKET SCRAPER - TESTING LOCAL ===")
    logger.info("Usuario: mgiglio")
    logger.info("Contraseña: ****")
    logger.info("URL de login: https://backoffice.puntoticket.com/Report...")
    logger.info("🎭 Solo eventos con fechas actuales o futuras")
    
    # Crear y ejecutar el scraper con Chrome visible para testing
    scraper = PuntoTicketScraper(headless=False)  # Para testing local
    result = scraper.run()
    
    if result:
        print("✅ Scraper ejecutado exitosamente")
        print(f"📊 Resultados: {result}")
    else:
        print("❌ Error en la ejecución del scraper")
    
    return result

if __name__ == "__main__":
    main()
