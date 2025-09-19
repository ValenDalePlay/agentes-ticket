 #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EntradaUno Scraper
Automatiza la extracción de datos de bordereaux desde el sistema EntradaUno
"""

import time
import json
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
import os

class EntradaUnoScraper:
    def __init__(self):
        self.setup_logging()
        self.driver = None
        self.wait = None
        self.base_url = "https://bo.entradauno.com"
        self.login_url = f"{self.base_url}/Home/Login?ReturnUrl=/Reporte/General/HistoricoDeVentas"
        self.bordeaux_url = f"{self.base_url}/Reporte/General/BorderauxEtiquetaPrecio"
        
        # Credenciales
        self.username = "fLauria"
        self.password = "lauria2021"
        
        # Configurar carpeta de salida
        self.output_dir = os.path.join(os.path.dirname(__file__), 'jsonentradauno')
        os.makedirs(self.output_dir, exist_ok=True)
        
    def setup_logging(self):
        """Configurar logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('entradauno_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_driver(self):
        """Configurar el driver de Chrome"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.wait = WebDriverWait(self.driver, 20)
            self.logger.info("Driver configurado correctamente")
            
        except Exception as e:
            self.logger.error(f"Error configurando driver: {e}")
            raise
            
    def login(self):
        """Realizar login en EntradaUno"""
        try:
            self.logger.info("Iniciando proceso de login")
            self.driver.get(self.login_url)
            time.sleep(3)
            
            # Hacer clic en "Siguiente" si existe
            try:
                siguiente_btn = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-login-default.btn-login.ripple"))
                )
                siguiente_btn.click()
                self.logger.info("Botón 'Siguiente' clickeado")
                time.sleep(2)
            except TimeoutException:
                self.logger.info("Botón 'Siguiente' no encontrado, continuando...")
            
            # Esperar a que aparezcan los campos de login
            time.sleep(2)
            
            # Llenar usuario
            username_field = self.wait.until(
                EC.presence_of_element_located((By.ID, "clogin"))
            )
            username_field.clear()
            username_field.send_keys(self.username)
            self.logger.info("Usuario ingresado")
            
            # Llenar contraseña
            password_field = self.driver.find_element(By.ID, "cclave")
            password_field.clear()
            password_field.send_keys(self.password)
            self.logger.info("Contraseña ingresada")
            
            # Hacer clic en "Ingresar"
            login_btn = self.driver.find_element(By.ID, "login")
            login_btn.click()
            self.logger.info("Botón 'Ingresar' clickeado")
            
            # Esperar a que cargue la página después del login
            time.sleep(5)
            
            # Verificar si el login fue exitoso
            if "Login" not in self.driver.current_url:
                self.logger.info("Login exitoso")
                return True
            else:
                self.logger.error("Login falló")
                return False
                
        except Exception as e:
            self.logger.error(f"Error durante el login: {e}")
            return False
            
    def navigate_to_bordeaux(self):
        """Navegar a la página de Bordeaux"""
        try:
            self.logger.info("Navegando a página de Bordeaux")
            self.driver.get(self.bordeaux_url)
            time.sleep(5)
            
            # Verificar que estamos en la página correcta
            if "BorderauxEtiquetaPrecio" in self.driver.current_url:
                self.logger.info("Navegación a Bordeaux exitosa")
                return True
            else:
                self.logger.error("Error navegando a Bordeaux")
                return False
                
        except Exception as e:
            self.logger.error(f"Error navegando a Bordeaux: {e}")
            return False
            
    def debug_page_structure(self):
        """Debug para inspeccionar la estructura de la página"""
        try:
            self.logger.info("=== DEBUG: Inspeccionando estructura de página ===")
            
            # Buscar todos los dropdowns
            dropdowns = self.driver.find_elements(By.CSS_SELECTOR, ".dx-selectbox")
            self.logger.info(f"Encontrados {len(dropdowns)} dropdowns")
            
            # Buscar selectores específicos
            establecimientos_elem = self.driver.find_elements(By.ID, "selectEstablecimiento")
            self.logger.info(f"Elemento selectEstablecimiento encontrado: {len(establecimientos_elem) > 0}")
            
            if establecimientos_elem:
                elem = establecimientos_elem[0]
                self.logger.info(f"Clases del elemento: {elem.get_attribute('class')}")
                self.logger.info(f"Estado habilitado: {elem.is_enabled()}")
                
                # Verificar si está deshabilitado
                if "dx-state-disabled" in elem.get_attribute('class'):
                    self.logger.warning("El dropdown de establecimientos está deshabilitado")
                    
            # Buscar overlays existentes
            overlays = self.driver.find_elements(By.CSS_SELECTOR, ".dx-overlay")
            self.logger.info(f"Encontrados {len(overlays)} overlays")
            
            # Verificar contenido de la página
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            if "Solo Vigentes" in page_text:
                self.logger.info("Texto 'Solo Vigentes' encontrado en la página")
            if "Establecimiento" in page_text:
                self.logger.info("Texto 'Establecimiento' encontrado en la página")
                
            self.logger.info("=== FIN DEBUG ===")
            
        except Exception as e:
            self.logger.error(f"Error en debug: {e}")
            
    def get_dropdown_options(self, dropdown_id):
        """Obtener opciones de un dropdown DevExtreme usando JavaScript"""
        try:
            self.logger.info(f"Obteniendo opciones para {dropdown_id}")
            
            # Usar JavaScript para obtener las opciones del dropdown
            script = f"""
            var dropdown = $("#{dropdown_id}").dxSelectBox("instance");
            if (dropdown) {{
                var dataSource = dropdown.option("dataSource");
                if (dataSource && Array.isArray(dataSource)) {{
                    return dataSource.map(function(item) {{
                        if (typeof item === 'string') {{
                            return {{ text: item, value: item }};
                        }} else if (item.text) {{
                            return {{ text: item.text, value: item.value || item.text }};
                        }} else {{
                            return {{ text: String(item), value: item }};
                        }}
                    }});
                }}
            }}
            return [];
            """
            
            options_data = self.driver.execute_script(script)
            
            if options_data:
                options = [{'text': opt['text'], 'value': opt['value']} for opt in options_data]
                self.logger.info(f"Encontradas {len(options)} opciones via JavaScript: {[opt['text'] for opt in options]}")
                return options
            
            # Si JavaScript no funciona, intentar método manual
            return self.get_dropdown_options_manual(dropdown_id)
            
        except Exception as e:
            self.logger.error(f"Error obteniendo opciones de {dropdown_id}: {e}")
            return self.get_dropdown_options_manual(dropdown_id)
            
    def get_dropdown_options_manual(self, dropdown_id):
        """Método manual para obtener opciones de dropdown"""
        try:
            # Hacer clic directamente en el input del dropdown
            dropdown_input = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, f"#{dropdown_id} .dx-texteditor-input"))
            )
            dropdown_input.click()
            time.sleep(3)
            
            # Buscar el overlay que aparece
            overlays = self.driver.find_elements(By.CSS_SELECTOR, ".dx-overlay:not(.dx-state-invisible)")
            
            options = []
            for overlay in overlays:
                try:
                    items = overlay.find_elements(By.CSS_SELECTOR, ".dx-list-item .dx-item-content")
                    for item in items:
                        text = item.get_attribute('textContent').strip()
                        if text and text != "No data to display":
                            options.append({'text': text, 'element': item.find_element(By.XPATH, "..")})
                    
                    if options:
                        break
                except:
                    continue
            
            # Cerrar dropdown
            self.driver.find_element(By.TAG_NAME, "body").click()
            time.sleep(1)
            
            self.logger.info(f"Encontradas {len(options)} opciones manualmente: {[opt['text'] for opt in options]}")
            return options
            
        except Exception as e:
            self.logger.error(f"Error en método manual para {dropdown_id}: {e}")
            return []
            
    def select_dropdown_option(self, dropdown_id, option_text):
        """Seleccionar una opción específica del dropdown DevExtreme"""
        try:
            self.logger.info(f"Seleccionando '{option_text}' en {dropdown_id}")
            
            # Método 1: Usar JavaScript para seleccionar directamente
            script = f"""
            var dropdown = $("#{dropdown_id}").dxSelectBox("instance");
            if (dropdown) {{
                var dataSource = dropdown.option("dataSource");
                if (dataSource && Array.isArray(dataSource)) {{
                    var option = dataSource.find(function(item) {{
                        var itemText = typeof item === 'string' ? item : item.text || String(item);
                        return itemText.includes("{option_text}") || itemText === "{option_text}";
                    }});
                    
                    if (option) {{
                        var value = typeof option === 'string' ? option : option.value || option.text;
                        dropdown.option("value", value);
                        
                        // Trigger change event
                        var changeEvent = new Event('change', {{ bubbles: true }});
                        dropdown.element()[0].dispatchEvent(changeEvent);
                        
                        return true;
                    }}
                }}
            }}
            return false;
            """
            
            result = self.driver.execute_script(script)
            
            if result:
                self.logger.info(f"Selección exitosa via JavaScript: '{option_text}' en {dropdown_id}")
                time.sleep(3)  # Esperar a que se procesen los cambios
                return True
                
            # Método 2: Selección manual si JavaScript falla
            return self.select_dropdown_option_manual(dropdown_id, option_text)
            
        except Exception as e:
            self.logger.error(f"Error seleccionando '{option_text}' en {dropdown_id}: {e}")
            return self.select_dropdown_option_manual(dropdown_id, option_text)
            
    def select_dropdown_option_manual(self, dropdown_id, option_text):
        """Método manual para seleccionar opción de dropdown"""
        try:
            # Hacer clic en el input del dropdown
            dropdown_input = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, f"#{dropdown_id} .dx-texteditor-input"))
            )
            dropdown_input.click()
            time.sleep(3)
            
            # Buscar la opción en todos los overlays visibles
            overlays = self.driver.find_elements(By.CSS_SELECTOR, ".dx-overlay:not(.dx-state-invisible)")
            
            option_element = None
            for overlay in overlays:
                try:
                    items = overlay.find_elements(By.CSS_SELECTOR, ".dx-list-item")
                    for item in items:
                        content = item.find_element(By.CSS_SELECTOR, ".dx-item-content")
                        text = content.get_attribute('textContent').strip()
                        if option_text in text or text == option_text:
                            option_element = item
                            break
                    
                    if option_element:
                        break
                except:
                    continue
            
            if option_element:
                # Hacer scroll al elemento si es necesario
                self.driver.execute_script("arguments[0].scrollIntoView(true);", option_element)
                time.sleep(1)
                
                # Intentar múltiples estrategias de clic
                clicked = False
                
                # Estrategia 1: Clic normal
                try:
                    option_element.click()
                    clicked = True
                    self.logger.info("Clic manual normal exitoso")
                except Exception as e:
                    self.logger.warning(f"Clic normal falló: {e}")
                
                # Estrategia 2: JavaScript click
                if not clicked:
                    try:
                        self.driver.execute_script("arguments[0].click();", option_element)
                        clicked = True
                        self.logger.info("Clic manual JavaScript exitoso")
                    except Exception as e:
                        self.logger.warning(f"Clic JavaScript falló: {e}")
                
                if clicked:
                    time.sleep(3)  # Esperar a que se procese la selección
                    self.logger.info(f"Selección manual exitosa: '{option_text}' en {dropdown_id}")
                    return True
                else:
                    self.logger.error(f"Todas las estrategias de clic manual fallaron para '{option_text}'")
                    return False
            else:
                self.logger.error(f"No se encontró la opción '{option_text}' en {dropdown_id}")
                # Cerrar dropdown
                try:
                    self.driver.find_element(By.TAG_NAME, "body").click()
                except:
                    pass
                return False
                
        except Exception as e:
            self.logger.error(f"Error en selección manual de '{option_text}' en {dropdown_id}: {e}")
            # Cerrar dropdown en caso de error
            try:
                self.driver.find_element(By.TAG_NAME, "body").click()
            except:
                pass
            return False
            
    def force_dropdown_selection(self, dropdown_id, option_value):
        """Forzar selección usando valores directos"""
        try:
            self.logger.info(f"Forzando selección en {dropdown_id} con valor {option_value}")
            
            # Script para forzar la selección
            script = f"""
            var dropdown = $("#{dropdown_id}").dxSelectBox("instance");
            if (dropdown) {{
                dropdown.option("value", "{option_value}");
                
                // Disparar eventos
                var element = dropdown.element()[0];
                var events = ['change', 'input', 'blur'];
                events.forEach(function(eventType) {{
                    var event = new Event(eventType, {{ bubbles: true }});
                    element.dispatchEvent(event);
                }});
                
                return true;
            }}
            return false;
            """
            
            result = self.driver.execute_script(script)
            
            if result:
                self.logger.info(f"Selección forzada exitosa en {dropdown_id}")
                time.sleep(3)
                return True
            else:
                self.logger.error(f"Selección forzada falló en {dropdown_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error en selección forzada: {e}")
            return False
            
    def wait_for_data_load(self, timeout=15):
        """Esperar a que los datos se carguen completamente"""
        try:
            # Esperar a que aparezca la tabla principal
            self.wait.until(
                EC.presence_of_element_located((By.ID, "gridReportes"))
            )
            
            # Esperar a que no haya indicadores de carga
            max_attempts = timeout
            for attempt in range(max_attempts):
                try:
                    # Verificar si hay indicadores de carga visibles
                    loading_indicators = self.driver.find_elements(By.CSS_SELECTOR, ".dx-loadindicator:not(.dx-state-invisible)")
                    
                    if not loading_indicators:
                        # Verificar que hay contenido en las tablas
                        main_table_rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridReportes .dx-datagrid-rowsview tbody tr")
                        summary_table_rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridReportesDOS .dx-datagrid-rowsview tbody tr")
                        
                        if len(main_table_rows) > 0 or len(summary_table_rows) > 0:
                            self.logger.info(f"Datos cargados correctamente. Filas principales: {len(main_table_rows)}, Filas resumen: {len(summary_table_rows)}")
                            time.sleep(2)  # Pequeña pausa adicional
                            return True
                        else:
                            self.logger.info(f"Intento {attempt + 1}: Esperando que aparezcan datos en las tablas...")
                    else:
                        self.logger.info(f"Intento {attempt + 1}: Detectados {len(loading_indicators)} indicadores de carga activos...")
                        
                except Exception as e:
                    self.logger.warning(f"Error verificando carga en intento {attempt + 1}: {e}")
                
                time.sleep(1)
            
            self.logger.warning(f"Timeout después de {timeout} segundos esperando carga completa de datos")
            return False
            
        except TimeoutException:
            self.logger.warning("Timeout esperando que aparezca la tabla principal")
            return False
            
    def extract_main_table_data(self):
        """Extraer datos de la tabla principal"""
        try:
            main_table_data = []
            
            # Intentar diferentes selectores para las filas de la tabla principal
            selectors = [
                "#gridReportes .dx-datagrid-rowsview tbody tr.dx-row.main-row",
                "#gridReportes .dx-datagrid-rowsview tbody tr.dx-row",
                "#gridReportes tbody tr",
                "#gridReportes .dx-datagrid-rowsview tr"
            ]
            
            rows = []
            for selector in selectors:
                try:
                    found_rows = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if found_rows:
                        rows = found_rows
                        self.logger.info(f"Encontradas {len(found_rows)} filas con selector: {selector}")
                        break
                except:
                    continue
            
            if not rows:
                self.logger.warning("No se encontraron filas en la tabla principal con ningún selector")
                return []
            
            for i, row in enumerate(rows):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 8:
                        row_data = {
                            'promocion': cells[0].text.strip(),
                            'capacidad': cells[1].text.strip(),
                            'vendidas': cells[2].text.strip(),
                            'invitaciones': cells[3].text.strip(),
                            'bloqueadas': cells[4].text.strip(),
                            'kills': cells[5].text.strip(),
                            'disponibles': cells[6].text.strip(),
                            'total': cells[7].text.strip()
                        }
                        # Solo agregar si tiene contenido útil
                        if any(value != '-' and value != '' for value in row_data.values()):
                            main_table_data.append(row_data)
                    else:
                        self.logger.debug(f"Fila {i} tiene solo {len(cells)} celdas, esperado al menos 8")
                except Exception as e:
                    self.logger.warning(f"Error procesando fila {i}: {e}")
                    
            self.logger.info(f"Extraídas {len(main_table_data)} filas válidas de la tabla principal")
            return main_table_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de tabla principal: {e}")
            return []
            
    def extract_summary_table_data(self):
        """Extraer datos de la tabla de resumen (segunda tabla)"""
        try:
            summary_data = []
            
            # Buscar filas de la tabla de resumen
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridReportesDOS .dx-datagrid-rowsview tbody tr.dx-row")
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 3:
                    row_data = {
                        'descuento': cells[0].text.strip(),
                        'ingresos': cells[1].text.strip(),
                        'total': cells[2].text.strip()
                    }
                    summary_data.append(row_data)
                    
            self.logger.info(f"Extraídas {len(summary_data)} filas de la tabla de resumen")
            return summary_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de tabla de resumen: {e}")
            return []
            
    def extract_payment_methods_data(self):
        """Extraer datos de la tabla de formas de pago"""
        try:
            payment_data = []
            
            # Buscar filas de la tabla de formas de pago
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridReportesTRES .dx-datagrid-rowsview tbody tr.dx-row")
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 3:
                    row_data = {
                        'forma_pago': cells[0].text.strip(),
                        'cantidad_tickets': cells[1].text.strip(),
                        'total': cells[2].text.strip()
                    }
                    payment_data.append(row_data)
                    
            self.logger.info(f"Extraídas {len(payment_data)} filas de la tabla de formas de pago")
            return payment_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de tabla de formas de pago: {e}")
            return []
            
    def extract_totals_data(self):
        """Extraer datos de totales y resumen"""
        try:
            totals_data = {}
            
            # Extraer datos del resumen lateral
            try:
                vendidas = self.driver.find_element(By.ID, "vendidas").text.strip()
                disponibles = self.driver.find_element(By.ID, "disponibles").text.strip()
                invitaciones = self.driver.find_element(By.ID, "invitaciones").text.strip()
                capacidad = self.driver.find_element(By.ID, "capacidad").text.strip()
                bloqueadas = self.driver.find_element(By.ID, "bloqueadas").text.strip()
                kills = self.driver.find_element(By.ID, "kills").text.strip()
                devoluciones = self.driver.find_element(By.ID, "devoluciones").text.strip()
                montototal = self.driver.find_element(By.ID, "montototal").text.strip()
                totaldevoluciones = self.driver.find_element(By.ID, "totaldevoluciones").text.strip()
                totalventas = self.driver.find_element(By.ID, "totalventas").text.strip()
                
                totals_data = {
                    'vendidas': vendidas,
                    'disponibles': disponibles,
                    'invitaciones': invitaciones,
                    'capacidad': capacidad,
                    'bloqueadas': bloqueadas,
                    'kills': kills,
                    'devueltas': devoluciones,
                    'monto_total': montototal,
                    'monto_total_devuelto': totaldevoluciones,
                    'venta_total_neta': totalventas
                }
                
            except NoSuchElementException as e:
                self.logger.warning(f"Algunos elementos de totales no encontrados: {e}")
                
            self.logger.info("Datos de totales extraídos")
            return totals_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de totales: {e}")
            return {}
            
    def extract_event_info(self):
        """Extraer información del evento"""
        try:
            event_info = {}
            
            try:
                nombre_reporte = self.driver.find_element(By.ID, "nombre_reporte").text.strip()
                nombre_locacion = self.driver.find_element(By.ID, "nombre_locacion").text.strip()
                
                event_info = {
                    'nombre_evento': nombre_reporte,
                    'locacion_fecha': nombre_locacion
                }
                
            except NoSuchElementException:
                self.logger.warning("Información de evento no encontrada")
                
            return event_info
            
        except Exception as e:
            self.logger.error(f"Error extrayendo información de evento: {e}")
            return {}
            
    def extract_all_data(self, establecimiento, espectaculo, funcion):
        """Extraer todos los datos de la página actual"""
        try:
            # Pequeña pausa adicional antes de extraer
            self.logger.info("Pausa final antes de extracción...")
            time.sleep(3)
                
            # Extraer información del evento
            event_info = self.extract_event_info()
            
            # Extraer datos de todas las tablas
            main_table = self.extract_main_table_data()
            summary_table = self.extract_summary_table_data()
            payment_methods = self.extract_payment_methods_data()
            totals = self.extract_totals_data()
            
            # Compilar todos los datos
            all_data = {
                'timestamp': datetime.now().isoformat(),
                'establecimiento': establecimiento,
                'espectaculo': espectaculo,
                'funcion': funcion,
                'evento_info': event_info,
                'tabla_principal': main_table,
                'tabla_resumen': summary_table,
                'formas_pago': payment_methods,
                'totales': totals
            }
            
            return all_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo todos los datos: {e}")
            return None
            
    def save_data_to_json(self, data, filename):
        """Guardar datos en archivo JSON"""
        try:
            # Verificar que el directorio existe
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir, exist_ok=True)
                self.logger.info(f"Directorio creado: {self.output_dir}")
            
            filepath = os.path.join(self.output_dir, filename)
            self.logger.info(f"Intentando guardar en: {filepath}")
            
            # Verificar que hay datos para guardar
            if not data:
                self.logger.error("No hay datos para guardar")
                return False
                
            self.logger.info(f"Datos a guardar - Tamaño: {len(str(data))} caracteres")
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            # Verificar que el archivo se creó
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                self.logger.info(f"Archivo guardado exitosamente: {filepath} ({file_size} bytes)")
                return True
            else:
                self.logger.error(f"El archivo no se creó: {filepath}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error guardando datos en {filename}: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False
            
    def get_current_selections(self):
        """Obtener las selecciones actuales de los dropdowns"""
        try:
            selections = {}
            
            # Establecimiento
            try:
                est_input = self.driver.find_element(By.CSS_SELECTOR, "#selectEstablecimiento input[type='hidden']")
                est_value = est_input.get_attribute('value')
                est_text = self.driver.find_element(By.CSS_SELECTOR, "#selectEstablecimiento .dx-texteditor-input").get_attribute('value')
                selections['establecimiento'] = {'value': est_value, 'text': est_text}
            except:
                selections['establecimiento'] = None
                
            # Espectáculo
            try:
                esp_input = self.driver.find_element(By.CSS_SELECTOR, "#selectEspectaculo input[type='hidden']")
                esp_value = esp_input.get_attribute('value')
                esp_text = self.driver.find_element(By.CSS_SELECTOR, "#selectEspectaculo .dx-texteditor-input").get_attribute('value')
                selections['espectaculo'] = {'value': esp_value, 'text': esp_text}
            except:
                selections['espectaculo'] = None
                
            # Función
            try:
                fun_input = self.driver.find_element(By.CSS_SELECTOR, "#selectFuncion input[type='hidden']")
                fun_value = fun_input.get_attribute('value')
                fun_text = self.driver.find_element(By.CSS_SELECTOR, "#selectFuncion .dx-texteditor-input").get_attribute('value')
                selections['funcion'] = {'value': fun_value, 'text': fun_text}
            except:
                selections['funcion'] = None
                
            return selections
            
        except Exception as e:
            self.logger.error(f"Error obteniendo selecciones actuales: {e}")
            return {}
            
    def check_if_data_available(self):
        """Verificar si hay datos disponibles en las tablas"""
        try:
            # Verificar si hay datos en la tabla principal
            main_table_rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridReportes .dx-datagrid-rowsview tbody tr")
            if len(main_table_rows) > 0:
                return True
            return False
        except:
            return False
    
    def run_complete_scraping(self):
        """Ejecutar el scraping completo"""
        try:
            self.setup_driver()
            
            # Login
            if not self.login():
                self.logger.error("Login falló, terminando")
                return False
                
            # Navegar a bordeaux
            if not self.navigate_to_bordeaux():
                self.logger.error("Navegación a bordeaux falló, terminando")
                return False
                
            # Debug de la estructura de la página
            self.debug_page_structure()
            
            # Verificar las selecciones actuales
            current_selections = self.get_current_selections()
            self.logger.info(f"Selecciones actuales: {current_selections}")
            
            # No extraer datos iniciales, solo cuando se apliquen filtros
            self.logger.info("Saltando extracción inicial, solo extraerá con filtros aplicados")
                
            # Usar valores conocidos basados en el HTML proporcionado
            # Establecimiento: "Metropolitano Rosario" (value: 145)
            # Espectáculo: "Morat" (value: 15886) 
            # Funciones: "11 de Oct del 2025 21:00h", "12 de Oct del 2025 21:00h"
            
            establecimientos_conocidos = [
                {'text': 'Metropolitano Rosario', 'value': '145'}
            ]
            
            espectaculos_conocidos = {
                'Metropolitano Rosario': [
                    {'text': 'Morat', 'value': '15886'}
                ]
            }
            
            funciones_conocidas = {
                'Morat': [
                    {'text': '11 de Oct del 2025 21:00h', 'value': '45184'},
                    {'text': '12 de Oct del 2025 21:00h', 'value': '45185'}  # Valor estimado
                ]
            }
            
            # Intentar obtener opciones dinámicamente primero
            establecimientos = self.get_dropdown_options("selectEstablecimiento")
            if not establecimientos:
                self.logger.info("Usando establecimientos conocidos")
                establecimientos = establecimientos_conocidos
                
            total_extracciones = 0
            
            for establecimiento in establecimientos:
                self.logger.info(f"Procesando establecimiento: {establecimiento['text']}")
                
                # Seleccionar establecimiento usando método forzado si es necesario
                establecimiento_seleccionado = False
                
                # Intentar selección normal primero
                if 'value' in establecimiento:
                    establecimiento_seleccionado = self.force_dropdown_selection("selectEstablecimiento", establecimiento['value'])
                
                if not establecimiento_seleccionado:
                    establecimiento_seleccionado = self.select_dropdown_option("selectEstablecimiento", establecimiento['text'])
                
                if not establecimiento_seleccionado:
                    self.logger.warning(f"No se pudo seleccionar establecimiento {establecimiento['text']}, continuando...")
                    continue
                    
                time.sleep(3)  # Esperar a que se carguen los espectáculos
                
                # Obtener espectáculos - usar conocidos si es necesario
                espectaculos = self.get_dropdown_options("selectEspectaculo")
                if not espectaculos and establecimiento['text'] in espectaculos_conocidos:
                    self.logger.info("Usando espectáculos conocidos")
                    espectaculos = espectaculos_conocidos[establecimiento['text']]
                
                for espectaculo in espectaculos:
                    self.logger.info(f"Procesando espectáculo: {espectaculo['text']}")
                    
                    # Seleccionar espectáculo usando método forzado si es necesario
                    espectaculo_seleccionado = False
                    
                    if 'value' in espectaculo:
                        espectaculo_seleccionado = self.force_dropdown_selection("selectEspectaculo", espectaculo['value'])
                    
                    if not espectaculo_seleccionado:
                        espectaculo_seleccionado = self.select_dropdown_option("selectEspectaculo", espectaculo['text'])
                    
                    if not espectaculo_seleccionado:
                        self.logger.warning(f"No se pudo seleccionar espectáculo {espectaculo['text']}, continuando...")
                        continue
                        
                    time.sleep(3)  # Esperar a que se carguen las funciones
                    
                    # Obtener funciones - usar conocidas si es necesario
                    funciones = self.get_dropdown_options("selectFuncion")
                    if not funciones and espectaculo['text'] in funciones_conocidas:
                        self.logger.info("Usando funciones conocidas")
                        funciones = funciones_conocidas[espectaculo['text']]
                    
                    for funcion in funciones:
                        self.logger.info(f"Procesando función: {funcion['text']}")
                        
                        # Seleccionar función usando método forzado si es necesario
                        funcion_seleccionada = False
                        
                        if 'value' in funcion:
                            funcion_seleccionada = self.force_dropdown_selection("selectFuncion", funcion['value'])
                        
                        if not funcion_seleccionada:
                            funcion_seleccionada = self.select_dropdown_option("selectFuncion", funcion['text'])
                        
                        if not funcion_seleccionada:
                            self.logger.warning(f"No se pudo seleccionar función {funcion['text']}, continuando...")
                            continue
                            
                        # Esperar tiempo fijo para que carguen los datos
                        self.logger.info(f"Esperando 15 segundos para carga completa de datos...")
                        time.sleep(15)
                        
                        # Verificación simple de datos - no depender de indicadores de carga
                        self.logger.info("Verificando que existan elementos de tabla...")
                        main_table_exists = len(self.driver.find_elements(By.CSS_SELECTOR, "#gridReportes")) > 0
                        if main_table_exists:
                            self.logger.info("Tabla principal encontrada, procediendo con extracción")
                        else:
                            self.logger.warning("Tabla principal no encontrada, esperando 5 segundos más...")
                            time.sleep(5)
                        
                        # Extraer todos los datos
                        self.logger.info(f"Iniciando extracción de datos para función: {funcion['text']}")
                        data = self.extract_all_data(
                            establecimiento['text'],
                            espectaculo['text'],
                            funcion['text']
                        )
                        
                        if data:
                            # Mostrar resumen de datos extraídos
                            main_rows = len(data.get('tabla_principal', []))
                            summary_rows = len(data.get('tabla_resumen', []))
                            payment_rows = len(data.get('formas_pago', []))
                            
                            self.logger.info(f"Datos extraídos - Principal: {main_rows} filas, Resumen: {summary_rows} filas, Pagos: {payment_rows} filas")
                            
                            # Crear nombre de archivo
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            filename = f"entradauno_{establecimiento['text'].replace(' ', '_')}_{espectaculo['text'].replace(' ', '_')}_{funcion['text'][:10].replace(' ', '_')}_{timestamp}.json"
                            
                            # Guardar datos
                            if self.save_data_to_json(data, filename):
                                total_extracciones += 1
                        else:
                            self.logger.warning(f"No se pudieron extraer datos para función: {funcion['text']}")
                                
            self.logger.info(f"Scraping completo. Total de extracciones: {total_extracciones}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error en scraping completo: {e}")
            return False
            
        finally:
            if self.driver:
                self.driver.quit()
                
def main():
    """Función principal"""
    scraper = EntradaUnoScraper()
    
    try:
        success = scraper.run_complete_scraping()
        if success:
            print("Scraping completado exitosamente")
        else:
            print("Scraping falló")
            
    except KeyboardInterrupt:
        print("\nScraping interrumpido por el usuario")
    except Exception as e:
        print(f"Error inesperado: {e}")
    finally:
        if scraper.driver:
            scraper.driver.quit()

if __name__ == "__main__":
    main()
