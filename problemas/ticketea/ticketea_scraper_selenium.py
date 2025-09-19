#!/usr/bin/env python3
"""
Ticketea Scraper con Selenium WebDriver Visual 
Muestra el navegador mientras se ejecuta
"""

import time
import json
import logging
from datetime import datetime
import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ticketea_selenium_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TicketeaSeleniumScraper:
    def __init__(self):
        self.api_key = "af9e3756d516cdb21be7f0d4d5e83f8b"  # API key de 2captcha
        self.login_url = "https://ticketea.com.py/manage/sign_in"
        self.base_url = "https://ticketea.com.py"
        
        # Credenciales
        self.email = "camila.halfon@daleplay.la"
        self.password = "turquesa_oro"
        
        # Driver de Selenium
        self.driver = None
        
    def setup_driver(self):
        """
        Configura el driver de Chrome con ventana visible
        """
        print("🌐 Configurando navegador Chrome...")
        
        chrome_options = Options()
        
        # Configuración para mostrar el navegador
        chrome_options.add_argument("--start-maximized")  # Ventana maximizada
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # User agent real
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36")
        
        # Intentar múltiples métodos para ChromeDriver
        try:
            # Método 1: Usar path directo sin service
            self.driver = webdriver.Chrome(options=chrome_options)
            print("✅ Usando ChromeDriver por defecto del sistema")
        except Exception as e1:
            try:
                # Método 2: WebDriver Manager con versión específica
                service = Service(ChromeDriverManager(version="118.0.5993.70").install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                print("✅ Usando ChromeDriver v118 con WebDriver Manager")
            except Exception as e2:
                # Método 3: Intentar con path específico
                try:
                    service = Service("/usr/local/bin/chromedriver")
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                    print("✅ Usando ChromeDriver de /usr/local/bin/")
                except Exception as e3:
                    print(f"❌ No se pudo inicializar ChromeDriver:")
                    print(f"   Error 1: {e1}")
                    print(f"   Error 2: {e2}")
                    print(f"   Error 3: {e3}")
                    raise Exception("No se pudo inicializar ChromeDriver con ningún método")
        
        # Ejecutar script para evitar detección de automation
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        print("✅ Navegador Chrome configurado y listo!")
        
    def solve_turnstile_visual(self, sitekey):
        """
        Resuelve el Cloudflare Turnstile usando 2captcha pero manteniendo el navegador visible
        """
        print("\n🔐 Iniciando resolución de Cloudflare Turnstile...")
        
        # Crear tarea
        task_data = {
            "clientKey": self.api_key,
            "task": {
                "type": "TurnstileTaskProxyless",
                "websiteURL": self.login_url,
                "websiteKey": sitekey
            }
        }
        
        print("📤 Enviando tarea a 2captcha...")
        response = requests.post("https://api.2captcha.com/createTask", json=task_data)
        result = response.json()
        
        if result.get("errorId") != 0:
            print(f"❌ Error creando tarea: {result}")
            return None
            
        task_id = result.get("taskId")
        print(f"✅ Tarea creada con ID: {task_id}")
        
        # Esperar resultado mientras mostramos el navegador
        max_attempts = 30
        attempt = 0
        
        while attempt < max_attempts:
            # Mostrar progreso visual
            for i in range(5):
                time.sleep(1)
                print(f"⏳ Esperando resolución del Turnstile... {i+1}/5 segundos", end='\r')
            
            check_data = {
                "clientKey": self.api_key,
                "taskId": task_id
            }
            
            response = requests.post("https://api.2captcha.com/getTaskResult", json=check_data)
            result = response.json()
            
            print()  # Nueva línea
            
            if result.get("status") == "ready":
                token = result.get("solution", {}).get("token")
                print("🎉 Turnstile resuelto exitosamente!")
                print(f"✅ Token obtenido: {token[:50]}...")
                return token
            elif result.get("status") == "processing":
                print(f"🔄 Turnstile en proceso... intento {attempt + 1}/{max_attempts}")
                attempt += 1
            else:
                print(f"❌ Error en resolución: {result}")
                return None
                
        print("❌ Timeout esperando resolución del Turnstile")
        return None

    def login_visual(self):
        """
        Realiza el login de forma visual usando Selenium
        """
        print("\n🚀 Iniciando proceso de login visual...")
        
        try:
            # Ir a la página de login
            print("📖 Navegando a la página de login...")
            self.driver.get(self.login_url)
            time.sleep(3)  # Esperar a que cargue
            
            print("🔍 Buscando elementos del formulario...")
            
            # Buscar el sitekey del Turnstile
            try:
                turnstile_element = self.driver.find_element(By.CSS_SELECTOR, '[data-sitekey]')
                sitekey = turnstile_element.get_attribute('data-sitekey')
                print(f"🔑 Sitekey encontrado: {sitekey}")
            except:
                sitekey = "0x4AAAAAAADWGgqdTg_SY-mN"  # Fallback
                print(f"🔑 Usando sitekey predeterminado: {sitekey}")
            
            # Resolver Turnstile
            turnstile_token = self.solve_turnstile_visual(sitekey)
            if not turnstile_token:
                print("❌ No se pudo resolver el Turnstile")
                return False
            
            # Llenar el formulario
            print("📝 Llenando formulario de login...")
            
            # Email
            email_field = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "manager_email"))
            )
            email_field.clear()
            email_field.send_keys(self.email)
            print(f"✅ Email ingresado: {self.email}")
            
            # Password
            password_field = self.driver.find_element(By.ID, "manager_password")
            password_field.clear()
            password_field.send_keys(self.password)
            print("✅ Password ingresado")
            
            # Inyectar el token de Turnstile
            print("🔐 Inyectando token de Turnstile...")
            self.driver.execute_script(f"""
                // Inyectar token en los campos hidden
                let tokenField1 = document.querySelector('input[name="cf_turnstile_token"]');
                if (tokenField1) {{
                    tokenField1.value = '{turnstile_token}';
                }}
                
                let tokenField2 = document.querySelector('input[name="cf-turnstile-response"]');
                if (tokenField2) {{
                    tokenField2.value = '{turnstile_token}';
                }}
                
                // También intentar con el widget de Turnstile
                if (window.turnstile && window.turnstile.render) {{
                    console.log('Turnstile widget detectado');
                }}
            """)
            
            time.sleep(2)  # Pausa para que veas el formulario lleno
            
            # Enviar formulario
            print("🚀 Enviando formulario...")
            submit_button = self.driver.find_element(By.CSS_SELECTOR, 'input[type="submit"]')
            submit_button.click()
            
            # Esperar redirección
            print("⏳ Esperando redirección...")
            time.sleep(5)
            
            # Verificar si estamos logueados
            current_url = self.driver.current_url
            print(f"📍 URL actual: {current_url}")
            
            if "sign_in" not in current_url and "manage" in current_url:
                print("🎉 ¡Login exitoso!")
                return True
            else:
                print("❌ Login fallido")
                return False
                
        except Exception as e:
            print(f"❌ Error durante el login: {str(e)}")
            return False

    def extract_events_visual(self):
        """
        Extrae eventos de forma visual navegando por la interfaz y haciendo clic en cada evento
        """
        print("\n🔍 Extrayendo eventos de forma visual...")
        
        try:
            # Obtener timestamp para archivos
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Buscar enlaces de eventos específicos como AIRBAG
            print("🎯 Buscando enlaces de eventos...")
            
            # Buscar todos los enlaces que contengan rutas de eventos
            event_links = []
            
            # Buscar enlaces con href que contengan "/events/"
            links_with_events = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/events/')]")
            
            for link in links_with_events:
                try:
                    href = link.get_attribute('href')
                    # Verificar que no sea un enlace de "new" o "copy"
                    if href and '/events/' in href and not any(x in href for x in ['/new', '/copy']):
                        # Buscar el texto del evento dentro del enlace
                        event_title = ""
                        try:
                            title_element = link.find_element(By.TAG_NAME, "h2")
                            event_title = title_element.text.strip()
                        except:
                            event_title = link.text.strip()
                        
                        if event_title and len(event_title) > 3:
                            event_links.append({
                                'href': href,
                                'title': event_title,
                                'element': link
                            })
                            print(f"🎪 Evento encontrado: {event_title}")
                except Exception as e:
                    continue
            
            print(f"✅ Encontrados {len(event_links)} eventos para procesar")
            
            all_events_data = []
            
            for i, event_info in enumerate(event_links):
                print(f"\n📖 Procesando evento {i+1}/{len(event_links)}: {event_info['title']}")
                
                try:
                    # Hacer clic en el evento
                    self.driver.execute_script("arguments[0].click();", event_info['element'])
                    time.sleep(3)  # Esperar a que cargue la página
                    
                    # Extraer datos de la página del evento
                    event_data = self.extract_single_event_data(event_info['title'], event_info['href'])
                    
                    if event_data:
                        all_events_data.append(event_data)
                        print(f"✅ Datos extraídos de: {event_info['title']}")
                    
                    # Volver atrás para seguir con el siguiente evento
                    print("⬅️ Volviendo al dashboard...")
                    self.driver.back()
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"❌ Error procesando evento {event_info['title']}: {str(e)}")
                    # Intentar volver al dashboard si algo salió mal
                    try:
                        self.driver.get(f"{self.base_url}/manage")
                        time.sleep(2)
                    except:
                        pass
                    continue
            
            if all_events_data:
                print(f"\n🎊 ¡Procesados exitosamente {len(all_events_data)} eventos!")
                
                # Guardar todos los datos
                if not os.path.exists("jsonticketea"):
                    os.makedirs("jsonticketea")
                    
                filename = f"jsonticketea/ticketea_eventos_completos_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(all_events_data, f, ensure_ascii=False, indent=2)
                
                print(f"💾 Todos los datos guardados en: {filename}")
                return True
            else:
                print("⚠️ No se pudieron extraer datos de eventos")
                return False
                
        except Exception as e:
            print(f"❌ Error en extracción de eventos: {str(e)}")
            return False

    def extract_single_event_data(self, event_title, event_url):
        """
        Extrae todos los datos de una página individual de evento
        """
        try:
            print(f"🔍 Extrayendo datos de la página del evento...")
            
            # Datos básicos del evento
            event_data = {
                'title': event_title,
                'url': event_url,
                'timestamp': datetime.now().isoformat(),
                'sections': {}
            }
            
            # No tomar screenshots - solo procesar datos
            
            # Extraer información general del evento
            try:
                # Buscar fechas y horarios
                date_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '2025') or contains(text(), '2024')]")
                dates = []
                for elem in date_elements:
                    text = elem.text.strip()
                    if text and ('2024' in text or '2025' in text):
                        dates.append(text)
                event_data['dates'] = list(set(dates))
                
                # Buscar estado del evento
                status_elements = self.driver.find_elements(By.CLASS_NAME, "chip")
                status_info = []
                for elem in status_elements:
                    text = elem.text.strip()
                    if text:
                        status_info.append(text)
                event_data['status'] = status_info
                
            except Exception as e:
                print(f"⚠️ Error extrayendo info general: {e}")
            
            # Extraer datos de tickets/precios si están disponibles
            try:
                # Buscar tablas de precios
                tables = self.driver.find_elements(By.TAG_NAME, "table")
                for i, table in enumerate(tables):
                    try:
                        rows = table.find_elements(By.TAG_NAME, "tr")
                        if len(rows) > 1:
                            table_data = []
                            headers = []
                            
                            # Extraer headers
                            header_row = rows[0]
                            for th in header_row.find_elements(By.TAG_NAME, "th"):
                                headers.append(th.text.strip())
                            
                            if not headers:
                                for td in header_row.find_elements(By.TAG_NAME, "td"):
                                    headers.append(td.text.strip())
                            
                            # Extraer datos
                            for row in rows[1:]:
                                row_data = []
                                for td in row.find_elements(By.TAG_NAME, "td"):
                                    row_data.append(td.text.strip())
                                if row_data:
                                    table_data.append(row_data)
                            
                            if table_data:
                                event_data['sections'][f'tabla_{i}'] = {
                                    'headers': headers,
                                    'data': table_data
                                }
                    except:
                        continue
                        
            except Exception as e:
                print(f"⚠️ Error extrayendo tablas: {e}")
            
            # Extraer formularios y campos
            try:
                forms = self.driver.find_elements(By.TAG_NAME, "form")
                form_data = []
                for form in forms:
                    inputs = form.find_elements(By.TAG_NAME, "input")
                    selects = form.find_elements(By.TAG_NAME, "select")
                    
                    form_info = {
                        'action': form.get_attribute('action'),
                        'method': form.get_attribute('method'),
                        'inputs': [],
                        'selects': []
                    }
                    
                    for inp in inputs:
                        form_info['inputs'].append({
                            'name': inp.get_attribute('name'),
                            'type': inp.get_attribute('type'),
                            'value': inp.get_attribute('value'),
                            'placeholder': inp.get_attribute('placeholder')
                        })
                    
                    for sel in selects:
                        options = []
                        for option in sel.find_elements(By.TAG_NAME, "option"):
                            options.append({
                                'value': option.get_attribute('value'),
                                'text': option.text
                            })
                        form_info['selects'].append({
                            'name': sel.get_attribute('name'),
                            'options': options
                        })
                    
                    form_data.append(form_info)
                
                if form_data:
                    event_data['sections']['formularios'] = form_data
                    
            except Exception as e:
                print(f"⚠️ Error extrayendo formularios: {e}")
            
            # Extraer todos los enlaces
            try:
                links = self.driver.find_elements(By.TAG_NAME, "a")
                links_data = []
                for link in links:
                    href = link.get_attribute('href')
                    text = link.text.strip()
                    if href and text:
                        links_data.append({
                            'href': href,
                            'text': text
                        })
                
                if links_data:
                    event_data['sections']['enlaces'] = links_data[:20]  # Limitar a 20 enlaces
                    
            except Exception as e:
                print(f"⚠️ Error extrayendo enlaces: {e}")
            
            # Extraer texto completo de la página
            try:
                page_text = self.driver.find_element(By.TAG_NAME, "body").text
                # Guardar solo partes relevantes del texto
                lines = page_text.split('\n')
                relevant_lines = [line.strip() for line in lines if line.strip() and len(line.strip()) > 3]
                event_data['sections']['texto_completo'] = relevant_lines[:100]  # Primeras 100 líneas relevantes
                
            except Exception as e:
                print(f"⚠️ Error extrayendo texto: {e}")
            
            return event_data
            
        except Exception as e:
            print(f"❌ Error extrayendo datos del evento: {str(e)}")
            return None

    def run_visual_scraping(self):
        """
        Ejecuta el scraping completo de forma visual
        """
        try:
            # Configurar driver
            self.setup_driver()
            
            # Login
            if not self.login_visual():
                print("❌ No se pudo realizar el login")
                return False
            
            # Extraer eventos
            time.sleep(3)  # Pausa para que veas la página
            success = self.extract_events_visual()
            
            # Mantener navegador abierto por unos segundos
            print("\n⏸️ Manteniendo navegador abierto por 10 segundos para que veas los resultados...")
            time.sleep(10)
            
            return success
            
        except Exception as e:
            print(f"❌ Error inesperado: {str(e)}")
            return False
        finally:
            if self.driver:
                print("🔄 Cerrando navegador...")
                self.driver.quit()

def main():
    """
    Función principal
    """
    print("="*70)
    print("🎫 TICKETEA SCRAPER VISUAL CON SELENIUM WEBDRIVER 🎫")
    print("="*70)
    print("🔐 API 2captcha configurada")
    print("👤 Credenciales: camila.halfon@daleplay.la")
    print("🌐 Sitio: ticketea.com.py")
    print("👁️ NAVEGADOR VISIBLE - Podrás ver todo el proceso!")
    print("-"*70)
    
    scraper = TicketeaSeleniumScraper()
    
    try:
        success = scraper.run_visual_scraping()
        
        if success:
            print("\n" + "="*70)
            print("🎉 ¡SCRAPING VISUAL COMPLETADO EXITOSAMENTE! 🎉")
            print("="*70)
        else:
            print("\n" + "="*70)
            print("❌ ERROR DURANTE EL SCRAPING VISUAL")
            print("="*70)
            
    except Exception as e:
        print(f"\n❌ Error inesperado: {str(e)}")
    
    print("\n🏁 Fin del proceso visual")

if __name__ == "__main__":
    main()
