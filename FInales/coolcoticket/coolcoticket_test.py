#!/usr/bin/env python3
"""
CoolcoTicket Scraper - Modo Test
Solo extrae y muestra datos, no guarda archivos
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging
from datetime import datetime, timedelta
import json
import re

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CoolcoTicketTest:
    def __init__(self):
        self.driver = None
        
        # Credenciales de login
        self.username = "CamilaHalfon"
        self.password = "1234"
        self.login_url = "https://ticketing.coolco.io/backoffice/login"
        
    def setup_driver(self):
        """Configura el driver de Chrome"""
        try:
            print("🌐 Configurando navegador...")
            
            chrome_options = Options()
            chrome_options.add_argument("--start-maximized")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
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
        """Realiza el login en el sistema Coolco Ticket"""
        try:
            print("🚀 Iniciando login...")
            
            # Navegar a la página de login
            print(f"📱 Navegando a: {self.login_url}")
            self.driver.get(self.login_url)
            time.sleep(3)
            
            # PASO 1: Ingresar usuario
            print("👤 Ingresando usuario...")
            
            try:
                user_field = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "login"))
                )
                user_field.clear()
                user_field.send_keys(self.username)
                print(f"✅ Usuario ingresado: {self.username}")
            except:
                print("❌ No se encontró el campo de usuario")
                return False
            
            # Hacer clic en "Continuar"
            try:
                continue_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "btnLogin"))
                )
                continue_button.click()
                print("✅ Botón 'Continuar' clickeado")
                time.sleep(3)
            except:
                print("❌ No se encontró el botón 'Continuar'")
                return False
            
            # PASO 2: Ingresar contraseña
            print("🔐 Ingresando contraseña...")
            
            try:
                password_field = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "password"))
                )
                password_field.clear()
                password_field.send_keys(self.password)
                print("✅ Contraseña ingresada")
            except:
                print("❌ No se encontró el campo de contraseña")
                return False
            
            # Hacer clic en "Acceder"
            try:
                access_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "btnLoginPasswd"))
                )
                access_button.click()
                print("✅ Botón 'Acceder' clickeado")
                time.sleep(5)
            except:
                print("❌ No se encontró el botón 'Acceder'")
                return False
            
            # Verificar login exitoso
            current_url = self.driver.current_url
            print(f"🔗 URL actual: {current_url}")
            
            if "login" not in current_url.lower():
                print("🎉 Login exitoso!")
                return True
            else:
                print("❌ Login fallido")
                return False
                
        except Exception as e:
            print(f"❌ Error durante el login: {e}")
            return False
    
    def click_personalizado_button(self):
        """Hace clic en el botón 'Personalizado'"""
        try:
            print("🔧 Buscando botón 'Personalizado'...")
            
            try:
                personalizado_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "button-customdate"))
                )
                personalizado_button.click()
                print("✅ Botón 'Personalizado' clickeado")
                time.sleep(3)
                return True
            except:
                print("⚠️ No se encontró el botón 'Personalizado' por ID")
                
                # Intentar buscar por texto
                try:
                    personalizado_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Personalizado')]"))
                    )
                    personalizado_button.click()
                    print("✅ Botón 'Personalizado' encontrado por texto")
                    time.sleep(3)
                    return True
                except:
                    print("❌ No se encontró el botón 'Personalizado'")
                    return False
            
        except Exception as e:
            print(f"❌ Error con botón 'Personalizado': {e}")
            return False
    
    def configure_custom_dates(self):
        """Configura las fechas personalizadas"""
        try:
            print("📅 Configurando fechas personalizadas...")
            
            # Calcular fechas
            fecha_inicio = "01/01/2025"
            fecha_fin = (datetime.now() + timedelta(days=10)).strftime("%d/%m/%Y")
            
            print(f"📅 Fecha inicio: {fecha_inicio}")
            print(f"📅 Fecha fin: {fecha_fin}")
            
            # Configurar fecha inicio
            try:
                fecha_inicio_field = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "dateFrom"))
                )
                fecha_inicio_field.clear()
                fecha_inicio_field.send_keys(fecha_inicio)
                print("✅ Fecha de inicio configurada")
            except:
                print("⚠️ No se encontró campo de fecha inicio")
                return False
            
            # Configurar fecha fin
            try:
                fecha_fin_field = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.ID, "dateTo"))
                )
                fecha_fin_field.clear()
                fecha_fin_field.send_keys(fecha_fin)
                print("✅ Fecha de fin configurada")
            except:
                print("⚠️ No se encontró campo de fecha fin")
                return False
            
            # Buscar botón aplicar
            try:
                apply_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
                apply_button.click()
                print("✅ Fechas aplicadas")
                time.sleep(5)
                return True
            except:
                print("⚠️ No se encontró botón aplicar, continuando...")
                return True
            
        except Exception as e:
            print(f"❌ Error configurando fechas: {e}")
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
            
            print(f"📊 Tablas: {len(tables)}")
            print(f"📝 Formularios: {len(forms)}")
            print(f"🔘 Botones: {len(buttons)}")
            print(f"📦 Divs: {len(divs)}")
            
            # Buscar contenedores específicos
            try:
                top_sessions = self.driver.find_elements(By.ID, "top-sessions-sales")
                print(f"🎭 Contenedor top-sessions-sales: {len(top_sessions)}")
            except:
                print("⚠️ No se encontró contenedor top-sessions-sales")
            
            try:
                future_sessions = self.driver.find_elements(By.ID, "future-sessions")
                print(f"🔮 Marcador future-sessions: {len(future_sessions)}")
            except:
                print("⚠️ No se encontró marcador future-sessions")
            
            # Mostrar parte del texto
            try:
                body_text = self.driver.find_element(By.TAG_NAME, "body").text
                print(f"\n📝 Primeros 300 caracteres del contenido:")
                print("-" * 30)
                print(body_text[:300])
                print("-" * 30)
            except:
                print("⚠️ No se pudo extraer texto del body")
            
        except Exception as e:
            print(f"❌ Error analizando contenido: {e}")
    
    def extract_future_sessions_data(self):
        """Extrae datos de sesiones futuras"""
        try:
            print("\n🎭 EXTRAYENDO DATOS DE SESIONES FUTURAS:")
            print("="*50)
            
            extracted_data = []
            
            # Buscar contenedor principal
            try:
                container = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "top-sessions-sales"))
                )
                print("✅ Contenedor top-sessions-sales encontrado")
            except:
                print("❌ No se encontró contenedor top-sessions-sales")
                return []
            
            # Buscar sesiones con data-id-session
            try:
                sessions = self.driver.find_elements(By.CSS_SELECTOR, "[data-id-session]")
                print(f"🎫 Sesiones encontradas: {len(sessions)}")
                
                for i, session in enumerate(sessions):
                    try:
                        session_data = self.extract_session_data(session, i + 1)
                        if session_data:
                            extracted_data.append(session_data)
                            print(f"✅ Sesión {i + 1} extraída: {session_data.get('nombre', 'N/A')}")
                    except Exception as e:
                        print(f"⚠️ Error en sesión {i + 1}: {e}")
                        continue
                
            except Exception as e:
                print(f"❌ Error buscando sesiones: {e}")
            
            print(f"\n📊 Total sesiones extraídas: {len(extracted_data)}")
            return extracted_data
            
        except Exception as e:
            print(f"❌ Error extrayendo sesiones futuras: {e}")
            return []
    
    def extract_session_data(self, session_element, index):
        """Extrae datos de una sesión específica"""
        try:
            session_data = {
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
                print(f"  📅 Fecha: {session_data['dia']} {session_data['mes']}")
            except:
                session_data["mes"] = ""
                session_data["dia"] = ""
                print("  ⚠️ No se pudo extraer fecha")
            
            # Extraer nombre
            try:
                name_elem = session_element.find_element(By.CSS_SELECTOR, ".name-session")
                session_data["nombre"] = name_elem.text.strip()
                print(f"  🎭 Nombre: {session_data['nombre']}")
            except:
                session_data["nombre"] = ""
                print("  ⚠️ No se pudo extraer nombre")
            
            # Extraer recaudación
            try:
                money_elem = session_element.find_element(By.CSS_SELECTOR, ".p4money")
                session_data["recaudacion"] = money_elem.text.strip()
                session_data["amount"] = money_elem.get_attribute("data-amount")
                print(f"  💰 Recaudación: {session_data['recaudacion']}")
            except:
                session_data["recaudacion"] = ""
                session_data["amount"] = ""
                print("  ⚠️ No se pudo extraer recaudación")
            
            # Extraer entradas vendidas
            try:
                tickets_elem = session_element.find_element(By.CSS_SELECTOR, ".value-total span:not(.p4money)")
                tickets_text = tickets_elem.text.strip()
                session_data["entradas_vendidas"] = tickets_text.replace("", "").strip()
                print(f"  🎫 Entradas: {session_data['entradas_vendidas']}")
            except:
                session_data["entradas_vendidas"] = ""
                print("  ⚠️ No se pudo extraer entradas")
            
            # Extraer imagen
            try:
                img_elem = session_element.find_element(By.CSS_SELECTOR, ".banner img")
                session_data["imagen_url"] = img_elem.get_attribute("src")
                print(f"  🖼️ Imagen: {session_data['imagen_url'][:50]}...")
            except:
                session_data["imagen_url"] = ""
                print("  ⚠️ No se pudo extraer imagen")
            
            return session_data
            
        except Exception as e:
            print(f"❌ Error extrayendo datos de sesión: {e}")
            return None
    
    def print_extracted_data(self, data):
        """Imprime los datos extraídos de forma legible"""
        if not data:
            print("\n❌ No hay datos para mostrar")
            return
        
        print(f"\n📋 DATOS EXTRAÍDOS ({len(data)} sesiones):")
        print("="*60)
        
        for i, session in enumerate(data, 1):
            print(f"\n🎭 SESIÓN {i}:")
            print(f"  📅 Fecha: {session.get('dia', 'N/A')} {session.get('mes', 'N/A')}")
            print(f"  🎭 Nombre: {session.get('nombre', 'N/A')}")
            print(f"  💰 Recaudación: {session.get('recaudacion', 'N/A')}")
            print(f"  🎫 Entradas: {session.get('entradas_vendidas', 'N/A')}")
            print(f"  🆔 Data ID: {session.get('data_id_session', 'N/A')}")
            print(f"  🖼️ Imagen: {session.get('imagen_url', 'N/A')[:50]}...")
    
    def close_driver(self):
        """Cierra el driver"""
        try:
            if self.driver:
                self.driver.quit()
                print("🔄 Navegador cerrado")
        except Exception as e:
            print(f"❌ Error cerrando navegador: {e}")
    
    def run_test(self):
        """Ejecuta el test completo"""
        try:
            print("🧪 COOLCO TICKET - MODO TEST")
            print("="*50)
            
            # 1. Configurar driver
            if not self.setup_driver():
                return False
            
            # 2. Login
            if not self.login():
                return False
            
            # 3. Botón personalizado
            self.click_personalizado_button()
            
            # 4. Configurar fechas
            self.configure_custom_dates()
            
            # 5. Analizar contenido
            self.analyze_page_content()
            
            # 6. Extraer datos
            extracted_data = self.extract_future_sessions_data()
            
            # 7. Mostrar datos
            self.print_extracted_data(extracted_data)
            
            print(f"\n🎉 TEST COMPLETADO!")
            print(f"📊 Total sesiones extraídas: {len(extracted_data)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error en test: {e}")
            return False
        finally:
            self.close_driver()

def main():
    """Función principal"""
    test = CoolcoTicketTest()
    success = test.run_test()
    
    if success:
        print("\n✅ Test ejecutado exitosamente")
    else:
        print("\n❌ Error en el test")

if __name__ == "__main__":
    main()
