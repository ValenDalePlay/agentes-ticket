#!/usr/bin/env python3
"""
Ticketea Scraper Completo
Login, extraer datos, calcular diferencias y guardar en daily_sales
"""

import time
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from database_config import get_database_connection

class TicketeaScraper:
    def __init__(self):
        self.api_key = "af9e3756d516cdb21be7f0d4d5e83f8b"
        self.email = "camila.halfon@daleplay.la"
        self.password = "turquesa_oro"
        self.driver = None
        
    def setup_driver(self):
        """Configurar Chrome"""
        print("🌐 Configurando navegador...")
        
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            print("✅ Navegador configurado")
        except Exception as e:
            print(f"❌ Error configurando navegador: {e}")
            raise
    
    def solve_turnstile(self, sitekey):
        """Resolver Turnstile con 2captcha"""
        print("🔐 Resolviendo Turnstile...")
        
        task_data = {
            "clientKey": self.api_key,
            "task": {
                "type": "TurnstileTaskProxyless",
                "websiteURL": "https://ticketea.com.py/manage/sign_in",
                "websiteKey": sitekey
            }
        }
        
        response = requests.post("https://api.2captcha.com/createTask", json=task_data)
        result = response.json()
        
        if result.get("errorId") != 0:
            print(f"❌ Error creando tarea: {result}")
            return None
            
        task_id = result.get("taskId")
        print(f"✅ Tarea creada: {task_id}")
        
        # Esperar resultado
        for attempt in range(30):
            time.sleep(5)
            print(f"⏳ Esperando... {attempt + 1}/30")
            
            check_data = {"clientKey": self.api_key, "taskId": task_id}
            response = requests.post("https://api.2captcha.com/getTaskResult", json=check_data)
            result = response.json()
            
            if result.get("status") == "ready":
                token = result.get("solution", {}).get("token")
                print("🎉 Turnstile resuelto!")
                return token
            elif result.get("status") == "processing":
                continue
            else:
                print(f"❌ Error: {result}")
                return None
                
        print("❌ Timeout")
        return None
    
    def login(self):
        """Login en Ticketea"""
        print("🚀 Iniciando login...")
        
        try:
            self.driver.get("https://ticketea.com.py/manage/sign_in")
            time.sleep(3)
            
            # Buscar sitekey
            try:
                turnstile_element = self.driver.find_element(By.CSS_SELECTOR, '[data-sitekey]')
                sitekey = turnstile_element.get_attribute('data-sitekey')
            except:
                sitekey = "0x4AAAAAAADWGgqdTg_SY-mN"
            
            # Resolver Turnstile
            token = self.solve_turnstile(sitekey)
            if not token:
                return False
            
            # Llenar formulario
            email_field = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "manager_email"))
            )
            email_field.clear()
            email_field.send_keys(self.email)
            
            password_field = self.driver.find_element(By.ID, "manager_password")
            password_field.clear()
            password_field.send_keys(self.password)
            
            # Inyectar token
            self.driver.execute_script(f"""
                let tokenField1 = document.querySelector('input[name="cf_turnstile_token"]');
                if (tokenField1) tokenField1.value = '{token}';
                
                let tokenField2 = document.querySelector('input[name="cf-turnstile-response"]');
                if (tokenField2) tokenField2.value = '{token}';
            """)
            
            time.sleep(2)
            
            # Enviar formulario
            submit_button = self.driver.find_element(By.CSS_SELECTOR, 'input[type="submit"]')
            submit_button.click()
            
            time.sleep(5)
            
            # Verificar login
            current_url = self.driver.current_url
            if "sign_in" not in current_url and "manage" in current_url:
                print("🎉 Login exitoso!")
                return True
            else:
                print("❌ Login fallido")
                return False
                
        except Exception as e:
            print(f"❌ Error en login: {e}")
            return False
    
    def extract_event_data(self, event_url):
        """Extraer datos del evento"""
        print(f"🔍 Extrayendo datos del evento: {event_url}")
        
        try:
            self.driver.get(event_url)
            time.sleep(5)
            
            # Extraer nombre del artista
            artist_name = self.extract_artist_name()
            
            # Extraer fecha del evento
            event_date = self.extract_event_date()
            
            # Extraer tickets generados
            tickets_generated = self.extract_tickets()
            
            # Extraer total ventas
            total_sales = self.extract_sales()
            
            print(f"✅ Datos extraídos:")
            print(f"   🎤 Artista: {artist_name}")
            print(f"   📅 Fecha: {event_date}")
            print(f"   🎫 Tickets generados: {tickets_generated:,}")
            print(f"   💰 Total ventas: ₲{total_sales:,} PYG")
            
            return {
                'artist_name': artist_name,
                'event_date': event_date,
                'tickets_generated': tickets_generated,
                'total_sales': total_sales
            }
            
        except Exception as e:
            print(f"❌ Error extrayendo datos: {e}")
            return None
    
    def extract_artist_name(self):
        """Extraer nombre del artista desde la URL"""
        try:
            current_url = self.driver.current_url
            if '/events/' in current_url:
                url_parts = current_url.split('/events/')[-1]
                artist_from_url = url_parts.split('-')[0]
                return artist_from_url.upper()
            return "AIRBAG"
        except:
            return "AIRBAG"
    
    def extract_event_date(self):
        """Extraer fecha del evento"""
        try:
            date_element = self.driver.find_element(By.CSS_SELECTOR, ".date p.d-block")
            date_text = date_element.text.strip()
            
            date_match = re.search(r'(\d{1,2})\s+([A-Z]{3})', date_text)
            if date_match:
                day = date_match.group(1)
                month_abbr = date_match.group(2)
                
                month_map = {
                    'ENE': '01', 'FEB': '02', 'MAR': '03', 'ABR': '04',
                    'MAY': '05', 'JUN': '06', 'JUL': '07', 'AGO': '08',
                    'SEP': '09', 'OCT': '10', 'NOV': '11', 'DIC': '12'
                }
                
                month_num = month_map.get(month_abbr, '01')
                current_year = datetime.now().year
                
                return f"{current_year}-{month_num}-{day.zfill(2)}"
            return None
        except:
            return None
    
    def extract_tickets(self):
        """Extraer tickets generados"""
        try:
            tickets_element = self.driver.find_element(By.ID, "event-dashboard-total-sales-count")
            tickets_text = tickets_element.text.strip()
            tickets_clean = re.sub(r'[^\d]', '', tickets_text)
            return int(tickets_clean) if tickets_clean else 0
        except:
            return 0
    
    def extract_sales(self):
        """Extraer total de ventas"""
        try:
            sales_element = self.driver.find_element(By.ID, "event-dashboard-total-sales")
            sales_text = sales_element.text.strip()
            sales_clean = re.sub(r'[^\d]', '', sales_text)
            return int(sales_clean) if sales_clean else 0
        except:
            return 0
    
    def find_show_in_database(self, artist_name, event_date):
        """Buscar el show en la base de datos"""
        print(f"🔍 Buscando show en BD: {artist_name} - {event_date}")
        
        try:
            connection = get_database_connection()
            if not connection:
                print("❌ No se pudo conectar a la base de datos")
                return None
                
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = """
                SELECT id, artista, venue, fecha_show, ciudad, pais, capacidad_total, ticketera
                FROM shows 
                WHERE UPPER(artista) = UPPER(%s) 
                AND DATE(fecha_show) = %s
                AND estado = 'activo'
            """
            
            cursor.execute(query, (artist_name, event_date))
            result = cursor.fetchone()
            
            cursor.close()
            connection.close()
            
            if result:
                print(f"✅ Show encontrado en BD:")
                print(f"   🆔 ID: {result['id']}")
                print(f"   🎤 Artista: {result['artista']}")
                print(f"   🏟️ Venue: {result['venue']}")
                print(f"   📅 Fecha: {result['fecha_show']}")
                print(f"   🌍 Ciudad: {result['ciudad']}, {result['pais']}")
                print(f"   👥 Capacidad: {result['capacidad_total']}")
                return result
            else:
                print(f"❌ No se encontró show en BD")
                return None
                
        except Exception as e:
            print(f"❌ Error buscando en BD: {e}")
            return None
    
    def get_latest_daily_sales(self, show_id):
        """Obtener el último registro de daily_sales"""
        print("🔍 Obteniendo último registro de daily_sales...")
        
        try:
            connection = get_database_connection()
            if not connection:
                return None
                
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = """
                SELECT 
                    fecha_venta,
                    venta_total_acumulada,
                    recaudacion_total_ars
                FROM daily_sales 
                WHERE show_id = %s 
                ORDER BY fecha_venta DESC 
                LIMIT 1
            """
            
            cursor.execute(query, (show_id,))
            result = cursor.fetchone()
            
            cursor.close()
            connection.close()
            
            if result:
                print(f"✅ Último registro encontrado:")
                print(f"   📅 Fecha: {result['fecha_venta']}")
                print(f"   🎫 Tickets vendidos: {result['venta_total_acumulada']:,}")
                print(f"   💰 Recaudación total: ₲{result['recaudacion_total_ars']:,} PYG")
                return result
            else:
                print("ℹ️ No hay registros previos en daily_sales")
                return None
                
        except Exception as e:
            print(f"❌ Error obteniendo daily_sales: {e}")
            return None
    
    def calculate_and_save_daily_sales(self, show_id, scraper_data, show_capacity):
        """Calcular diferencias y guardar/actualizar en daily_sales"""
        print("\n🧮 CALCULANDO Y GUARDANDO DATOS DIARIOS:")
        print("="*50)
        
        # Obtener último registro
        last_record = self.get_latest_daily_sales(show_id)
        
        if not last_record:
            print("❌ No se puede calcular sin registro previo")
            return False
        
        # Calcular diferencias
        tickets_diff = scraper_data['tickets_generated'] - last_record['venta_total_acumulada']
        sales_diff = scraper_data['total_sales'] - last_record['recaudacion_total_ars']
        
        print(f"📊 DIFERENCIAS CALCULADAS:")
        print(f"   🎫 Tickets nuevos: {tickets_diff}")
        print(f"   💰 Ventas nuevas: ₲{sales_diff:,} PYG")
        
        # Si no hay diferencias, no hacer nada
        if tickets_diff == 0 and sales_diff == 0:
            print("ℹ️ No hay cambios en los datos. No se actualiza nada.")
            return True
        
        if tickets_diff > 0:
            precio_promedio = sales_diff / tickets_diff
            print(f"   💵 Precio promedio: ₲{precio_promedio:,.0f} PYG por ticket")
        else:
            precio_promedio = 0
            print(f"   💵 Precio promedio: ₲0 PYG por ticket")
        
        # Calcular métricas
        fecha_hoy = datetime.now().strftime('%Y-%m-%d')
        tickets_totales = scraper_data['tickets_generated']
        tickets_disponibles = show_capacity - tickets_totales
        ocupacion = (tickets_totales / show_capacity) * 100
        
        # Verificar si ya existe un registro para hoy
        try:
            connection = get_database_connection()
            if not connection:
                print("❌ No se pudo conectar a la base de datos")
                return False
                
            cursor = connection.cursor()
            
            # Verificar si existe registro para hoy
            check_query = """
                SELECT id FROM daily_sales 
                WHERE show_id = %s AND fecha_venta = %s
            """
            cursor.execute(check_query, (show_id, fecha_hoy))
            existing_record = cursor.fetchone()
            
            if existing_record:
                # ACTUALIZAR registro existente
                print("🔄 Actualizando registro existente...")
                
                update_query = """
                    UPDATE daily_sales SET
                        fecha_extraccion = %s,
                        venta_diaria = %s,
                        monto_diario_ars = %s,
                        venta_total_acumulada = %s,
                        recaudacion_total_ars = %s,
                        tickets_disponibles = %s,
                        porcentaje_ocupacion = %s,
                        precio_promedio_ars = %s,
                        updated_at = NOW()
                    WHERE show_id = %s AND fecha_venta = %s
                """
                
                values = (
                    fecha_hoy,  # fecha_extraccion
                    tickets_diff,
                    sales_diff,
                    tickets_totales,
                    scraper_data['total_sales'],
                    tickets_disponibles,
                    round(ocupacion, 2),
                    round(precio_promedio) if tickets_diff > 0 else 0,
                    show_id,
                    fecha_hoy
                )
                
                cursor.execute(update_query, values)
                connection.commit()
                
                print(f"\n✅ REGISTRO ACTUALIZADO EXITOSAMENTE:")
                print(f"   📅 Fecha: {fecha_hoy}")
                print(f"   🎫 Tickets vendidos hoy: {tickets_diff}")
                print(f"   💰 Ventas del día: ₲{sales_diff:,} PYG")
                print(f"   📊 Total acumulado: {tickets_totales:,} tickets")
                print(f"   💰 Total recaudado: ₲{scraper_data['total_sales']:,} PYG")
                print(f"   🏟️ Tickets disponibles: {tickets_disponibles:,}")
                print(f"   📈 Ocupación: {ocupacion:.2f}%")
                
            else:
                # INSERTAR nuevo registro
                print("➕ Insertando nuevo registro...")
                
                insert_query = """
                    INSERT INTO daily_sales (
                        show_id, fecha_venta, fecha_extraccion, venta_diaria, monto_diario_ars,
                        venta_total_acumulada, recaudacion_total_ars, tickets_disponibles,
                        porcentaje_ocupacion, precio_promedio_ars
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                
                values = (
                    show_id,
                    fecha_hoy,
                    fecha_hoy,  # fecha_extraccion
                    tickets_diff,
                    sales_diff,
                    tickets_totales,
                    scraper_data['total_sales'],
                    tickets_disponibles,
                    round(ocupacion, 2),
                    round(precio_promedio) if tickets_diff > 0 else 0
                )
                
                cursor.execute(insert_query, values)
                connection.commit()
                
                print(f"\n✅ NUEVO REGISTRO INSERTADO EXITOSAMENTE:")
                print(f"   📅 Fecha: {fecha_hoy}")
                print(f"   🎫 Tickets vendidos hoy: {tickets_diff}")
                print(f"   💰 Ventas del día: ₲{sales_diff:,} PYG")
                print(f"   📊 Total acumulado: {tickets_totales:,} tickets")
                print(f"   💰 Total recaudado: ₲{scraper_data['total_sales']:,} PYG")
                print(f"   🏟️ Tickets disponibles: {tickets_disponibles:,}")
                print(f"   📈 Ocupación: {ocupacion:.2f}%")
            
            cursor.close()
            connection.close()
            return True
            
        except Exception as e:
            print(f"❌ Error guardando en daily_sales: {e}")
            return False

    def process_event(self, event_url):
        """Procesar evento completo"""
        print(f"\n🎯 PROCESANDO EVENTO COMPLETO")
        print("="*60)
        
        # 1. Extraer datos del evento
        event_data = self.extract_event_data(event_url)
        if not event_data:
            print("❌ No se pudieron extraer datos del evento")
            return False
        
        # 2. Buscar show en base de datos
        show_data = self.find_show_in_database(
            event_data['artist_name'], 
            event_data['event_date']
        )
        
        if not show_data:
            print("❌ No se encontró el show en la base de datos")
            return False
        
        # 3. Guardar datos en daily_sales
        success = self.calculate_and_save_daily_sales(
            show_data['id'],
            event_data,
            show_data['capacidad_total']
        )
        
        if success:
            print(f"\n🎉 PROCESO COMPLETADO EXITOSAMENTE!")
            print(f"   Show ID: {show_data['id']}")
            print(f"   Tickets: {event_data['tickets_generated']:,}")
            print(f"   Ventas: ₲{event_data['total_sales']:,}")
            return True
        else:
            print(f"\n❌ Error guardando datos en daily_sales")
            return False
    
    def close(self):
        """Cerrar navegador"""
        if self.driver:
            self.driver.quit()
            print("🔄 Navegador cerrado")

def main():
    """Función principal"""
    print("🎫 TICKETEA SCRAPER COMPLETO")
    print("="*50)
    
    scraper = TicketeaScraper()
    
    try:
        # 1. Configurar navegador
        scraper.setup_driver()
        
        # 2. Login
        if scraper.login():
            # 3. Procesar evento
            event_url = "https://ticketea.com.py/manage/companies/G5Pro/events/airbag-el-club-de-la-pelea-tour"
            success = scraper.process_event(event_url)
            
            if success:
                print("\n🎉 ¡SCRAPER COMPLETADO EXITOSAMENTE!")
            else:
                print("\n❌ Error en el proceso del scraper")
        else:
            print("\n❌ Error en login")
    
    except Exception as e:
        print(f"\n❌ Error general: {e}")
    
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
