#!/usr/bin/env python3
"""
PuntoTicket Scraper - Versión Final
Extrae datos de shows futuros y los guarda en la base de datos
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging
from datetime import datetime, timedelta
import json
import re
import psycopg2
from psycopg2.extras import RealDictCursor

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PuntoTicketScraper:
    def __init__(self):
        self.driver = None
        
        # Credenciales de login
        self.username = "mgiglio"
        self.password = "FMJBBWT"
        self.login_url = "https://backoffice.puntoticket.com/Report?title=1.%20Resumen%20Ventas&reporteId=4&eventoId=BIZ183"
        
        # Configuración de base de datos
        self.db_config = {
            "user": "postgres.wawpmejhydmlxdofbsfs",
            "password": "Valen44043436?",
            "host": "aws-1-us-east-1.pooler.supabase.com",
            "port": "5432",
            "dbname": "postgres"
        }
        
        # IDs específicos de los shows de Santiago que queremos procesar
        self.target_shows = {
            "cazzu": "0f47063f-f57a-4a10-9d4a-6f7303da47b1",  # Cazzu - 2025-11-28
            "diego torres": "c0771f56-4b8b-4d78-b1d7-2a49144aca3b"  # Diego Torres - 2025-11-29
        }
    
    def get_database_connection(self):
        """Establece conexión con la base de datos"""
        try:
            connection = psycopg2.connect(
                user=self.db_config["user"],
                password=self.db_config["password"],
                host=self.db_config["host"],
                port=self.db_config["port"],
                dbname=self.db_config["dbname"]
            )
            return connection
        except Exception as e:
            print(f"❌ Error conectando a la base de datos: {e}")
            return None
    
    def setup_driver(self):
        """Configura el driver de Chrome"""
        try:
            print("🌐 Configurando navegador...")
            
            chrome_options = Options()
            chrome_options.add_argument("--start-maximized")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            try:
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as e:
                print(f"⚠️ Error con ChromeDriverManager: {e}")
                print("Intentando usar ChromeDriver del sistema...")
                self.driver = webdriver.Chrome(options=chrome_options)
            
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            print("✅ Navegador configurado")
            return True
            
        except Exception as e:
            print(f"❌ Error configurando el driver: {e}")
            return False
    
    def login(self):
        """Realiza el login en el sistema Punto Ticket"""
        try:
            print("🚀 Iniciando login...")
            
            # Navegar a la página de login
            print(f"📱 Navegando a: {self.login_url}")
            self.driver.get(self.login_url)
            time.sleep(5)
            
            # Verificar si ya estamos logueados
            current_url = self.driver.current_url
            print(f"🔗 URL actual: {current_url}")
            
            if "logon" not in current_url.lower() and "login" not in current_url.lower():
                print("✅ Ya estamos logueados o en la página correcta")
                return True
            
            # Buscar el formulario de login específico
            print("🔍 Buscando formulario de login...")
            
            try:
                login_form = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "logon-form"))
                )
                print("✅ Formulario de login encontrado")
            except:
                print("❌ No se encontró el formulario de login")
                return False
            
            # Buscar campo de usuario por ID específico
            try:
                user_field = self.driver.find_element(By.ID, "UserName")
                user_field.clear()
                user_field.send_keys(self.username)
                print(f"✅ Usuario ingresado: {self.username}")
            except:
                print("❌ No se encontró el campo de usuario (ID: UserName)")
                return False
            
            # Buscar campo de contraseña por ID específico
            try:
                password_field = self.driver.find_element(By.ID, "Password")
                password_field.clear()
                password_field.send_keys(self.password)
                print("✅ Contraseña ingresada")
            except:
                print("❌ No se encontró el campo de contraseña (ID: Password)")
                return False
            
            # Buscar botón de submit específico
            try:
                submit_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
                submit_button.click()
                print("✅ Botón 'Ingresar' clickeado")
                time.sleep(5)
            except:
                print("❌ No se encontró el botón 'Ingresar'")
                return False
            
            # Verificar login exitoso
            current_url = self.driver.current_url
            print(f"🔗 URL después del login: {current_url}")
            
            if "logon" not in current_url.lower() and "login" not in current_url.lower():
                print("🎉 Login exitoso!")
                return True
            else:
                print("❌ Login fallido")
                return False
                
        except Exception as e:
            print(f"❌ Error durante el login: {e}")
            return False
    
    def analyze_page_content(self):
        """Analiza el contenido de la página"""
        try:
            print("\n🔍 ANALIZANDO CONTENIDO DE LA PÁGINA:")
            print("="*50)
            
            print(f"📄 Título: {self.driver.title}")
            print(f"🔗 URL: {self.driver.current_url}")
            
            # Contar elementos
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            forms = self.driver.find_elements(By.TAG_NAME, "form")
            buttons = self.driver.find_elements(By.TAG_NAME, "button")
            divs = self.driver.find_elements(By.TAG_NAME, "div")
            selects = self.driver.find_elements(By.TAG_NAME, "select")
            
            print(f"📊 Tablas: {len(tables)}")
            print(f"📝 Formularios: {len(forms)}")
            print(f"🔘 Botones: {len(buttons)}")
            print(f"📦 Divs: {len(divs)}")
            print(f"📋 Selects: {len(selects)}")
            
            # Buscar elementos específicos de PuntoTicket
            try:
                report_containers = self.driver.find_elements(By.CSS_SELECTOR, ".report-container, .report-content, .data-table")
                print(f"📊 Contenedores de reporte: {len(report_containers)}")
            except:
                print("⚠️ No se encontraron contenedores de reporte específicos")
            
            try:
                data_tables = self.driver.find_elements(By.CSS_SELECTOR, "table.data, .data-table table, .report-table")
                print(f"📋 Tablas de datos: {len(data_tables)}")
            except:
                print("⚠️ No se encontraron tablas de datos específicas")
            
            # Mostrar parte del texto
            try:
                body_text = self.driver.find_element(By.TAG_NAME, "body").text
                print(f"\n📝 Primeros 500 caracteres del contenido:")
                print("-" * 30)
                print(body_text[:500])
                print("-" * 30)
            except:
                print("⚠️ No se pudo extraer texto del body")
            
        except Exception as e:
            print(f"❌ Error analizando contenido: {e}")
    
    def extract_date_from_event_text(self, event_text):
        """Extrae la fecha del evento del texto del evento"""
        try:
            
            return None
        except:
            return None
            
    def get_show_from_database(self, artista, venue, fecha_evento):
        """Busca un show en la base de datos por artista, venue y fecha"""
        try:
            print(f"🔍 Buscando show en BD: {artista} - {venue} - {fecha_evento}")
            
            # Verificar si es uno de nuestros shows objetivo
            artista_lower = artista.lower()
            if artista_lower in self.target_shows:
                show_id = self.target_shows[artista_lower]
                print(f"✅ Show objetivo encontrado: {artista} (ID: {show_id})")
                
                # Obtener datos completos del show
                connection = self.get_database_connection()
                if connection:
                    cursor = connection.cursor(cursor_factory=RealDictCursor)
                    cursor.execute("""
                        SELECT * FROM shows WHERE id = %s
                    """, (show_id,))
                    
                    show_data = cursor.fetchone()
                    cursor.close()
                    connection.close()
                    
                    if show_data:
                        return dict(show_data)
                    else:
                        print(f"⚠️ No se encontraron datos del show {show_id}")
                        return None
                else:
                    print("❌ No se pudo conectar a la base de datos")
                    return None
            else:
                print(f"⚠️ Show no es objetivo: {artista}")
                return None
            
        except Exception as e:
            print(f"⚠️ Error buscando show en BD: {e}")
            return None
    
    def get_latest_daily_sales(self, show_id):
        """Obtiene el último registro de daily_sales para un show"""
        try:
            print(f"📊 Obteniendo último registro de daily_sales para show: {show_id}")
            
            connection = self.get_database_connection()
            if connection:
                cursor = connection.cursor(cursor_factory=RealDictCursor)
                cursor.execute("""
                    SELECT * FROM daily_sales 
                    WHERE show_id = %s 
                    ORDER BY fecha_venta DESC 
                    LIMIT 1
                """, (show_id,))
                
                last_record = cursor.fetchone()
                cursor.close()
                connection.close()
                
                if last_record:
                    print(f"✅ Último registro encontrado: {last_record['fecha_venta']}")
                    return dict(last_record)
                else:
                    print(f"⚠️ No se encontraron registros de daily_sales para show {show_id}")
                    return None
            else:
                print("❌ No se pudo conectar a la base de datos")
                return None
            
        except Exception as e:
            print(f"⚠️ Error obteniendo daily_sales: {e}")
            return None

    def calculate_daily_sales_data(self, current_data, last_record, show_capacity):
        """Calcula los datos de daily_sales basándose en diferencias"""
        try:
            print(f"🧮 Calculando datos de daily_sales...")
            
            # Datos actuales del scraper
            current_total_entradas = int(current_data.get('total_entradas', '0').replace('.', ''))
            current_total_monto = int(current_data.get('total_monto', '0').replace('.', ''))
            
            # Datos del último registro (si existe)
            if last_record:
                last_total_entradas = last_record.get('venta_total_acumulada', 0)
                last_total_monto = last_record.get('recaudacion_total_ars', 0)
            else:
                last_total_entradas = 0
                last_total_monto = 0
            
            # Calcular diferencias
            venta_diaria = current_total_entradas - last_total_entradas
            monto_diario = current_total_monto - last_total_monto
            
            # Calcular campos adicionales
            tickets_disponibles = show_capacity - current_total_entradas
            porcentaje_ocupacion = (current_total_entradas / show_capacity * 100) if show_capacity > 0 else 0
            precio_promedio = (current_total_monto / current_total_entradas) if current_total_entradas > 0 else 0
            
            daily_sales_data = {
                'fecha_venta': datetime.now().date(),
                'venta_diaria': venta_diaria,
                'monto_diario_ars': monto_diario,
                'venta_total_acumulada': current_total_entradas,
                'recaudacion_total_ars': current_total_monto,
                'tickets_disponibles': tickets_disponibles,
                'porcentaje_ocupacion': round(porcentaje_ocupacion, 2),
                'precio_promedio_ars': round(precio_promedio),
                'fecha_extraccion': datetime.now()
            }
            
            print(f"✅ Datos calculados:")
            print(f"  📊 Venta diaria: {venta_diaria}")
            print(f"  💰 Monto diario: ${monto_diario:,}")
            print(f"  🎫 Total acumulado: {current_total_entradas}")
            print(f"  💵 Total recaudado: ${current_total_monto:,}")
            print(f"  🎟️ Disponibles: {tickets_disponibles}")
            print(f"  📈 Ocupación: {porcentaje_ocupacion:.2f}%")
            print(f"  💲 Precio promedio: ${precio_promedio:,.0f}")
            
            return daily_sales_data
            
        except Exception as e:
            print(f"❌ Error calculando daily_sales: {e}")
            return None
    
    def save_daily_sales_data(self, show_id, daily_sales_data):
        """Guarda los datos de daily_sales con lógica inteligente de UPDATE/INSERT"""
        try:
            print(f"💾 Guardando datos de daily_sales para show: {show_id}")
            
            connection = self.get_database_connection()
            if not connection:
                print("❌ No se pudo conectar a la base de datos")
                return False
            
            cursor = connection.cursor()
            fecha_hoy = datetime.now().date()
            
            # Verificar si ya existe un registro para hoy
            cursor.execute("""
                SELECT * FROM daily_sales 
                WHERE show_id = %s AND fecha_venta = %s
            """, (show_id, fecha_hoy))
            
            existing_record = cursor.fetchone()
            
            if existing_record:
                # Ya existe un registro para hoy - verificar si hay diferencias
                print(f"📅 Registro existente encontrado para {fecha_hoy}")
                
                # Comparar datos
                existing_venta_total = existing_record[8]  # venta_total_acumulada
                existing_recaudacion = existing_record[9]  # recaudacion_total_ars
                
                new_venta_total = daily_sales_data['venta_total_acumulada']
                new_recaudacion = daily_sales_data['recaudacion_total_ars']
                
                # Calcular nuevas diferencias
                new_venta_diaria = new_venta_total - existing_venta_total
                new_monto_diario = new_recaudacion - existing_recaudacion
                
                # Solo actualizar si hay diferencias
                if new_venta_diaria != 0 or new_monto_diario != 0:
                    print(f"🔄 Actualizando registro con diferencias:")
                    print(f"  📊 Venta diaria: {new_venta_diaria}")
                    print(f"  💰 Monto diario: ${new_monto_diario:,}")
                    
                    # Actualizar el registro existente
                    cursor.execute("""
                        UPDATE daily_sales SET
                            venta_diaria = %s,
                            monto_diario_ars = %s,
                            venta_total_acumulada = %s,
                            recaudacion_total_ars = %s,
                            tickets_disponibles = %s,
                            porcentaje_ocupacion = %s,
                            precio_promedio_ars = %s,
                            fecha_extraccion = %s,
                            updated_at = NOW()
                        WHERE show_id = %s AND fecha_venta = %s
                    """, (
                        new_venta_diaria,
                        new_monto_diario,
                        new_venta_total,
                        new_recaudacion,
                        daily_sales_data['tickets_disponibles'],
                        daily_sales_data['porcentaje_ocupacion'],
                        daily_sales_data['precio_promedio_ars'],
                        daily_sales_data['fecha_extraccion'],
                        show_id,
                        fecha_hoy
                    ))
                    
                    connection.commit()
                    print("✅ Registro actualizado exitosamente")
                else:
                    print("ℹ️ No hay diferencias, manteniendo registro existente")
            else:
                # No existe registro para hoy - insertar nuevo
                print(f"📝 Insertando nuevo registro para {fecha_hoy}")
                
                cursor.execute("""
                    INSERT INTO daily_sales (
                        show_id, fecha_venta, fecha_extraccion,
                        venta_diaria, monto_diario_ars, monto_diario_usd,
                        venta_total_acumulada, recaudacion_total_ars, recaudacion_total_usd,
                        tickets_disponibles, porcentaje_ocupacion,
                        precio_promedio_ars, precio_promedio_usd,
                        ticketera, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                    )
                """, (
                    show_id,
                    daily_sales_data['fecha_venta'],
                    daily_sales_data['fecha_extraccion'],
                    daily_sales_data['venta_diaria'],
                    daily_sales_data['monto_diario_ars'],
                    "0.00",  # monto_diario_usd
                    daily_sales_data['venta_total_acumulada'],
                    daily_sales_data['recaudacion_total_ars'],
                    "0.00",  # recaudacion_total_usd
                    daily_sales_data['tickets_disponibles'],
                    daily_sales_data['porcentaje_ocupacion'],
                    daily_sales_data['precio_promedio_ars'],
                    "0.00",  # precio_promedio_usd
                    "puntoticket"
                ))
                
                connection.commit()
                print("✅ Nuevo registro insertado exitosamente")
            
            cursor.close()
            connection.close()
            return True
            
        except Exception as e:
            print(f"❌ Error guardando daily_sales: {e}")
            if connection:
                connection.rollback()
                connection.close()
            return False

    def get_available_events(self):
        """Obtiene la lista de eventos disponibles en el dropdown"""
        try:
            print("\n📋 OBTENIENDO EVENTOS DISPONIBLES:")
            print("="*50)
            
            # Buscar el dropdown de eventos (Select2)
            try:
                # Primero intentar con el selector original
                event_select = self.driver.find_element(By.CSS_SELECTOR, "select[name*='evento'], select[id*='evento'], select[name*='Event'], select[id*='Event']")
                select_obj = Select(event_select)
                options = [option.text.strip() for option in select_obj.options if option.text.strip()]
                print(f"✅ Eventos encontrados (Select normal): {len(options)}")
                return options
            except:
                # Si no funciona, buscar el Select2
                try:
                    # Buscar el elemento Select2
                    select2_container = self.driver.find_element(By.CSS_SELECTOR, ".select2-selection")
                    select2_container.click()
                    
                    # Esperar a que se abra el dropdown
                    time.sleep(2)
                    
                    # Buscar las opciones en el dropdown abierto
                    options_elements = self.driver.find_elements(By.CSS_SELECTOR, ".select2-results__option")
                    options = [option.text.strip() for option in options_elements if option.text.strip()]
                    
                    # Cerrar el dropdown haciendo click fuera
                    self.driver.find_element(By.TAG_NAME, "body").click()
                    time.sleep(1)
                    
                    print(f"✅ Eventos encontrados (Select2): {len(options)}")
                    return options
                except Exception as e:
                    print(f"⚠️ Error con Select2: {e}")
                    return []
                    
        except Exception as e:
            print(f"❌ Error obteniendo eventos: {e}")
            return []

    def select_event(self, event_text):
        """Selecciona un evento específico del dropdown y hace click en Ver reporte"""
        try:
            print(f"\n🎯 SELECCIONANDO EVENTO: {event_text}")
            print("="*50)
            
            # Buscar el dropdown de eventos
            try:
                # Primero intentar con el selector original
                event_select = self.driver.find_element(By.CSS_SELECTOR, "select[name*='evento'], select[id*='evento'], select[name*='Event'], select[id*='Event']")
                select_obj = Select(event_select)
                select_obj.select_by_visible_text(event_text)
                print(f"✅ Evento seleccionado: {event_text}")
            except:
                # Si no funciona, usar Select2
                try:
                    # Buscar el elemento Select2
                    select2_container = self.driver.find_element(By.CSS_SELECTOR, ".select2-selection")
                    select2_container.click()
                    
                    # Esperar a que se abra el dropdown
                    time.sleep(2)
                    
                    # Buscar y hacer click en la opción específica
                    option = self.driver.find_element(By.XPATH, f"//li[contains(@class, 'select2-results__option') and contains(text(), '{event_text}')]")
                    option.click()
                    
                    # Esperar a que se cierre el dropdown
                    time.sleep(2)
                    
                    print(f"✅ Evento seleccionado: {event_text}")
                except Exception as e:
                    print(f"⚠️ Error seleccionando evento: {e}")
                    return False
            
            # Ahora hacer click en el botón "Ver reporte"
            try:
                print("🔍 Buscando botón 'Ver reporte'...")
                
                # Buscar el botón "Ver reporte" por diferentes selectores
                ver_reporte_btn = None
                
                # Intentar por texto del botón
                try:
                    ver_reporte_btn = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Ver reporte')]")
                    print("✅ Botón 'Ver reporte' encontrado por texto")
                except:
                    # Intentar por clase CSS
                    try:
                        ver_reporte_btn = self.driver.find_element(By.CSS_SELECTOR, "button.btn-success")
                        print("✅ Botón 'Ver reporte' encontrado por clase CSS")
                    except:
                        # Intentar por ng-click
                        try:
                            ver_reporte_btn = self.driver.find_element(By.CSS_SELECTOR, "button[ng-click*='onBtnGetData']")
                            print("✅ Botón 'Ver reporte' encontrado por ng-click")
                        except:
                            print("⚠️ No se encontró el botón 'Ver reporte'")
                            return False
                
                if ver_reporte_btn:
                    # Hacer click en el botón
                    ver_reporte_btn.click()
                    print("✅ Click en 'Ver reporte' realizado")
                    
                    # Esperar a que se carguen los datos
                    time.sleep(3)
                    print("⏳ Esperando carga de datos...")
                    
                    return True
                else:
                    print("❌ No se pudo encontrar el botón 'Ver reporte'")
                    return False
                    
            except Exception as e:
                print(f"⚠️ Error haciendo click en 'Ver reporte': {e}")
                return False
            
        except Exception as e:
            print(f"❌ Error seleccionando evento: {e}")
            return False

    def extract_specific_data(self):
        """Extrae solo los datos específicos que necesitamos"""
        try:
            print("\n🎯 EXTRAYENDO DATOS ESPECÍFICOS:")
            print("="*50)
            
            # Datos que vamos a extraer
            extracted_data = {
                "fecha_extraccion": datetime.now().isoformat(),
                "url": self.driver.current_url,
                "titulo_pagina": self.driver.title,
                "pais": "Chile",
                "ticketera": "PuntoTicket"
            }
            
            # 1. Extraer información del evento desde el selector
            try:
                # Buscar el texto del evento seleccionado
                event_text = "N/A"
                
                # Intentar obtener el texto del Select2
                try:
                    select2_rendered = self.driver.find_element(By.CSS_SELECTOR, "#select2-eventoId-container")
                    event_text = select2_rendered.text.strip()
                    print(f"🎭 Evento seleccionado (Select2): {event_text}")
                except:
                    # Fallback al selector normal
                    try:
                        event_select = self.driver.find_element(By.CSS_SELECTOR, "select[name*='evento'], select[id*='evento'], select[name*='Event'], select[id*='Event']")
                        select_obj = Select(event_select)
                        selected_option = select_obj.first_selected_option
                        event_text = selected_option.text.strip()
                        print(f"🎭 Evento seleccionado (Select): {event_text}")
                    except:
                        print("⚠️ No se pudo obtener el evento seleccionado")
                
                # Parsear el texto del evento para extraer artista y venue
                # Formato: "BIZ183 - Duki - Ya Supiste Tour - Movistar Arena"
                if " - " in event_text and event_text != "N/A":
                    parts = event_text.split(" - ")
                    if len(parts) >= 4:
                        extracted_data["artista"] = parts[1].strip()  # Duki
                        extracted_data["venue"] = parts[3].strip()    # Movistar Arena
                        print(f"🎤 Artista: {extracted_data['artista']}")
                        print(f"🏟️ Venue: {extracted_data['venue']}")
                    else:
                        print("⚠️ Formato de evento no reconocido")
                        extracted_data["artista"] = "N/A"
                        extracted_data["venue"] = "N/A"
                else:
                    print("⚠️ Formato de evento no reconocido")
                    extracted_data["artista"] = "N/A"
                    extracted_data["venue"] = "N/A"
                
            except Exception as e:
                print(f"⚠️ Error extrayendo info del evento: {e}")
                extracted_data["artista"] = "N/A"
                extracted_data["venue"] = "N/A"
            
            # 2. Extraer fecha del evento desde las tablas
            try:
                tables = self.driver.find_elements(By.TAG_NAME, "table")
                fecha_evento = "N/A"
                
                for table in tables:
                    try:
                        # Buscar la primera fila de datos (no header)
                        rows = table.find_elements(By.TAG_NAME, "tr")
                        if len(rows) > 1:
                            first_data_row = rows[1]  # Segunda fila (primera de datos)
                            cells = first_data_row.find_elements(By.TAG_NAME, "td")
                            if len(cells) >= 2:
                                fecha_cell = cells[1]  # Segunda columna (Fecha Evento)
                                fecha_evento = fecha_cell.text.strip()
                                if fecha_evento and fecha_evento != "Total":
                                    print(f"📅 Fecha del evento: {fecha_evento}")
                                    break
                    except:
                        continue
                
                extracted_data["fecha_evento"] = fecha_evento
                
            except Exception as e:
                print(f"⚠️ Error extrayendo fecha: {e}")
                extracted_data["fecha_evento"] = "N/A"
            
            # 3. Extraer totales de entradas y monto
            try:
                tables = self.driver.find_elements(By.TAG_NAME, "table")
                total_entradas = "N/A"
                total_monto = "N/A"
                
                for table_index, table in enumerate(tables):
                    try:
                        # Buscar la fila "Total Evento"
                        rows = table.find_elements(By.TAG_NAME, "tr")
                        for row in rows:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) >= 2:
                                first_cell = cells[0].text.strip()
                                if "Total Evento" in first_cell or "Total" in first_cell:
                                    # Esta es la fila de totales
                                    last_cell = cells[-1].text.strip()  # Última columna
                                    
                                    if table_index == 0:  # Primera tabla = entradas
                                        total_entradas = last_cell
                                        print(f"🎫 Total entradas: {total_entradas}")
                                    elif table_index == 1:  # Segunda tabla = monto
                                        total_monto = last_cell
                                        print(f"💰 Total monto: {total_monto}")
                                    break
                    except:
                        continue
                
                extracted_data["total_entradas"] = total_entradas
                extracted_data["total_monto"] = total_monto
                
            except Exception as e:
                print(f"⚠️ Error extrayendo totales: {e}")
                extracted_data["total_entradas"] = "N/A"
                extracted_data["total_monto"] = "N/A"
            
            return extracted_data
            
        except Exception as e:
            print(f"❌ Error extrayendo datos específicos: {e}")
            return {}
    
    
    def print_extracted_data(self, specific_data):
        """Imprime los datos extraídos de forma legible"""
        print(f"\n📋 DATOS EXTRAÍDOS:")
        print("="*60)
        
        if specific_data:
            print(f"\n🎯 INFORMACIÓN DEL EVENTO:")
            print(f"  🎤 Artista: {specific_data.get('artista', 'N/A')}")
            print(f"  🏟️ Venue: {specific_data.get('venue', 'N/A')}")
            print(f"  📅 Fecha: {specific_data.get('fecha_evento', 'N/A')}")
            print(f"  🎫 Total Entradas: {specific_data.get('total_entradas', 'N/A')}")
            print(f"  💰 Total Monto: {specific_data.get('total_monto', 'N/A')}")
            print(f"  🌍 País: {specific_data.get('pais', 'N/A')}")
            print(f"  🎫 Ticketera: {specific_data.get('ticketera', 'N/A')}")
            
            print(f"\n📊 METADATOS:")
            print(f"  📄 Título: {specific_data.get('titulo_pagina', 'N/A')}")
            print(f"  🔗 URL: {specific_data.get('url', 'N/A')}")
            print(f"  ⏰ Fecha extracción: {specific_data.get('fecha_extraccion', 'N/A')}")
        else:
            print("❌ No se pudieron extraer datos")

    def print_all_events_data(self, all_events_data):
        """Imprime un resumen de todos los eventos procesados"""
        print(f"\n📊 RESUMEN DE TODOS LOS EVENTOS:")
        print("="*80)
        
        if not all_events_data:
            print("❌ No se procesaron eventos")
            return
        
        print(f"🎯 Total eventos procesados: {len(all_events_data)}")
        print(f"\n📋 DATOS POR EVENTO:")
        print("-" * 80)
        
        for i, event_data in enumerate(all_events_data, 1):
            print(f"\n🎭 EVENTO {i}:")
            print(f"  🎤 Artista: {event_data.get('artista', 'N/A')}")
            print(f"  🏟️ Venue: {event_data.get('venue', 'N/A')}")
            print(f"  📅 Fecha: {event_data.get('fecha_evento', 'N/A')}")
            print(f"  🎫 Total Entradas: {event_data.get('total_entradas', 'N/A')}")
            print(f"  💰 Total Monto: {event_data.get('total_monto', 'N/A')}")
            print(f"  🌍 País: {event_data.get('pais', 'N/A')}")
            print(f"  🎫 Ticketera: {event_data.get('ticketera', 'N/A')}")
            print(f"  📝 Evento completo: {event_data.get('evento_completo', 'N/A')}")
            
            # Mostrar datos de daily_sales si existen
            if 'daily_sales_data' in event_data:
                ds_data = event_data['daily_sales_data']
                print(f"\n  📊 DATOS QUE SE CARGARÍAN EN DAILY_SALES:")
                print(f"    📅 Fecha venta: {ds_data.get('fecha_venta', 'N/A')}")
                print(f"    🎫 Venta diaria: {ds_data.get('venta_diaria', 'N/A')}")
                print(f"    💰 Monto diario: ${ds_data.get('monto_diario_ars', 'N/A'):,}")
                print(f"    📈 Total acumulado: {ds_data.get('venta_total_acumulada', 'N/A')}")
                print(f"    💵 Total recaudado: ${ds_data.get('recaudacion_total_ars', 'N/A'):,}")
                print(f"    🎟️ Disponibles: {ds_data.get('tickets_disponibles', 'N/A')}")
                print(f"    📊 Ocupación: {ds_data.get('porcentaje_ocupacion', 'N/A')}%")
                print(f"    💲 Precio promedio: ${ds_data.get('precio_promedio_ars', 'N/A'):,}")
            else:
                print(f"  ⚠️ No hay datos de daily_sales para este evento")
        
        # Resumen estadístico
        print(f"\n📊 ESTADÍSTICAS:")
        print("-" * 40)
        
        # Contar artistas únicos
        artistas = [event.get('artista', 'N/A') for event in all_events_data if event.get('artista') != 'N/A']
        artistas_unicos = list(set(artistas))
        print(f"🎤 Artistas únicos: {len(artistas_unicos)}")
        
        # Contar venues únicos
        venues = [event.get('venue', 'N/A') for event in all_events_data if event.get('venue') != 'N/A']
        venues_unicos = list(set(venues))
        print(f"🏟️ Venues únicos: {len(venues_unicos)}")
        
        # Sumar total de entradas
        total_entradas = 0
        for event in all_events_data:
            try:
                entradas = event.get('total_entradas', '0').replace('.', '').replace(',', '')
                if entradas.isdigit():
                    total_entradas += int(entradas)
            except:
                continue
        print(f"🎫 Total entradas vendidas: {total_entradas:,}")
        
        # Sumar total de monto
        total_monto = 0
        for event in all_events_data:
            try:
                monto = event.get('total_monto', '0').replace('.', '').replace(',', '')
                if monto.isdigit():
                    total_monto += int(monto)
            except:
                continue
        print(f"💰 Total monto recaudado: ${total_monto:,}")
    
    def close_driver(self):
        """Cierra el driver"""
        try:
            if self.driver:
                self.driver.quit()
                print("🔄 Navegador cerrado")
        except Exception as e:
            print(f"❌ Error cerrando navegador: {e}")
    
    def run_scraper(self):
        """Ejecuta el scraper completo"""
        try:
            print("🎯 PUNTO TICKET - SCRAPER FINAL")
            print("="*50)
            
            # 1. Configurar driver
            if not self.setup_driver():
                return False
            
            # 2. Login
            if not self.login():
                return False
            
            # 3. Analizar contenido
            self.analyze_page_content()
            
            # 4. Obtener eventos disponibles
            available_events = self.get_available_events()
            if not available_events:
                print("❌ No se encontraron eventos disponibles")
                return False
            
            print(f"\n📋 EVENTOS DISPONIBLES ({len(available_events)}):")
            for i, event in enumerate(available_events[:5]):  # Mostrar solo los primeros 5
                print(f"  {i+1}. {event}")
            if len(available_events) > 5:
                print(f"  ... y {len(available_events) - 5} más")
            
            # 5. Procesar todos los eventos y filtrar por fecha después
            all_events_data = []
            today = datetime.now().date()
            
            print("🔍 Procesando todos los eventos para obtener fechas y datos...")
            
            for i, event_text in enumerate(available_events):
                try:
                    print(f"\n🎯 PROCESANDO EVENTO {i+1}/{len(available_events)}: {event_text}")
                    print("="*80)
                    
                    # Seleccionar el evento
                    if self.select_event(event_text):
                        # Esperar a que se carguen los datos
                        time.sleep(3)
                        
                        # Extraer datos del evento
                        event_data = self.extract_specific_data()
                        if event_data:
                            event_data["evento_completo"] = event_text
                            
                            # Verificar si es un evento futuro
                            fecha_str = event_data.get('fecha_evento', '')
                            is_future_event = False
                            
                            if fecha_str and fecha_str != 'N/A' and fecha_str != 'Total':
                                try:
                                    # Parsear fecha (formato: "2025-11-28 21:00")
                                    event_date = datetime.strptime(fecha_str.split(' ')[0], "%Y-%m-%d").date()
                                    if event_date > today:
                                        is_future_event = True
                                        print(f"✅ Evento futuro detectado: {event_date}")
                                    else:
                                        print(f"⏰ Evento pasado: {event_date}")
                                except:
                                    print(f"⚠️ Error parseando fecha: {fecha_str}")
                            
                            # Solo procesar eventos futuros Y que sean shows objetivo
                            artista_lower = event_data.get('artista', '').lower()
                            is_target_show = artista_lower in self.target_shows
                            
                            if is_future_event and is_target_show:
                                print(f"🎯 Procesando show objetivo: {event_data.get('artista', '')}")
                                
                                # Buscar el show en la base de datos
                                show_info = self.get_show_from_database(
                                    event_data.get('artista', ''),
                                    event_data.get('venue', ''),
                                    event_data.get('fecha_evento', '')
                                )
                                
                                if show_info:
                                    # Obtener último registro de daily_sales
                                    last_record = self.get_latest_daily_sales(show_info.get('id'))
                                    
                                    # Calcular datos de daily_sales
                                    daily_sales_data = self.calculate_daily_sales_data(
                                        event_data, 
                                        last_record, 
                                        show_info.get('capacidad_total', 0)
                                    )
                                    
                                    if daily_sales_data:
                                        event_data["daily_sales_data"] = daily_sales_data
                                        event_data["show_info"] = show_info
                                        print(f"✅ Evento futuro procesado con datos de BD")
                                        
                                        # Guardar datos en la base de datos
                                        if self.save_daily_sales_data(show_info.get('id'), daily_sales_data):
                                            print(f"✅ Datos guardados exitosamente en BD")
                                        else:
                                            print(f"❌ Error guardando datos en BD")
                                    else:
                                        print(f"⚠️ No se pudieron calcular datos de daily_sales")
                                else:
                                    print(f"⚠️ Show no encontrado en BD")
                                
                                all_events_data.append(event_data)
                                print(f"✅ Evento objetivo procesado exitosamente")
                            elif is_future_event and not is_target_show:
                                print(f"⏭️ Evento futuro pero no es objetivo: {event_data.get('artista', '')}")
                            else:
                                print(f"⏭️ Evento pasado, saltando...")
                        else:
                            print(f"⚠️ No se pudieron extraer datos del evento {i+1}")
                    else:
                        print(f"❌ No se pudo seleccionar el evento {i+1}")
                    
                    # Pequeña pausa entre eventos
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"❌ Error procesando evento {i+1}: {e}")
                    continue
            
            # 6. Mostrar resumen de todos los eventos
            self.print_all_events_data(all_events_data)
            
            print(f"\n🎉 SCRAPER COMPLETADO!")
            print(f"🎯 Eventos procesados: {len(all_events_data)}/{len(available_events)}")
            print(f"📋 Total eventos disponibles: {len(available_events)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error en scraper: {e}")
            return False
        finally:
            self.close_driver()

def main():
    """Función principal"""
    scraper = PuntoTicketScraper()
    success = scraper.run_scraper()
    
    if success:
        print("\n✅ Scraper ejecutado exitosamente")
    else:
        print("\n❌ Error en el scraper")

if __name__ == "__main__":
    main()
