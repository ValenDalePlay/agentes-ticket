#!/usr/bin/env python3
"""
Ticketea Scraper con Cloudflare Turnstile bypass usando 2captcha
"""

import requests
import time
import json
import logging
from datetime import datetime
import os

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ticketea_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TicketeaScraper:
    def __init__(self):
        self.api_key = "af9e3756d516cdb21be7f0d4d5e83f8b"  # API key de 2captcha
        self.login_url = "https://ticketea.com.py/manage/sign_in"
        self.base_url = "https://ticketea.com.py"
        self.session = requests.Session()
        
        # Headers comunes
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Credenciales
        self.email = "camila.halfon@daleplay.la"
        self.password = "turquesa_oro"

    def solve_turnstile(self, sitekey, url):
        """
        Resuelve el Cloudflare Turnstile usando 2captcha
        """
        print("\n🔐 Iniciando resolución de Cloudflare Turnstile...")
        logger.info("Iniciando resolución de Cloudflare Turnstile...")
        
        # Crear tarea
        task_data = {
            "clientKey": self.api_key,
            "task": {
                "type": "TurnstileTaskProxyless",
                "websiteURL": url,
                "websiteKey": sitekey
            }
        }
        
        print("📤 Enviando tarea a 2captcha...")
        # Enviar tarea a 2captcha
        response = requests.post("https://api.2captcha.com/createTask", json=task_data)
        result = response.json()
        
        if result.get("errorId") != 0:
            logger.error(f"❌ Error creando tarea: {result}")
            return None
            
        task_id = result.get("taskId")
        print(f"✅ Tarea creada con ID: {task_id}")
        logger.info(f"Tarea creada con ID: {task_id}")
        
        # Esperar resultado
        max_attempts = 30
        attempt = 0
        
        while attempt < max_attempts:
            # Esperar menos tiempo y mostrar progreso
            for i in range(5):  # 5 segundos total, pero con indicador visual
                time.sleep(1)
                print(f"⏳ Esperando resolución del Turnstile... {i+1}/5 segundos", end='\r')
            
            check_data = {
                "clientKey": self.api_key,
                "taskId": task_id
            }
            
            response = requests.post("https://api.2captcha.com/getTaskResult", json=check_data)
            result = response.json()
            
            print()  # Nueva línea después del contador
            
            if result.get("status") == "ready":
                token = result.get("solution", {}).get("token")
                logger.info("🎉 Turnstile resuelto exitosamente!")
                print(f"✅ Token obtenido: {token[:50]}...")
                return token
            elif result.get("status") == "processing":
                logger.info(f"🔄 Turnstile en proceso... intento {attempt + 1}/{max_attempts}")
                attempt += 1
            else:
                logger.error(f"❌ Error en resolución: {result}")
                return None
                
        logger.error("Timeout esperando resolución del Turnstile")
        return None

    def get_login_page(self):
        """
        Obtiene la página de login y extrae datos necesarios
        """
        logger.info("Obteniendo página de login...")
        
        response = self.session.get(self.login_url)
        
        if response.status_code != 200:
            logger.error(f"Error obteniendo página de login: {response.status_code}")
            return None, None, None
            
        # Verificar encoding y decodificar correctamente
        logger.info(f"Response encoding: {response.encoding}")
        logger.info(f"Content-Encoding header: {response.headers.get('content-encoding', 'None')}")
        
        # Obtener el contenido decodificado
        html_content = response.text
        
        # Verificar si el contenido está bien decodificado
        logger.info(f"Primeros 200 caracteres del HTML: {html_content[:200]}")
        
        # Si parece que está mal decodificado, intentar manual
        if len(html_content) < 100 or not '<html' in html_content.lower():
            try:
                import brotli
                if response.headers.get('content-encoding') == 'br':
                    html_content = brotli.decompress(response.content).decode('utf-8')
                    logger.info("Decodificación manual de Brotli exitosa")
            except ImportError:
                logger.warning("Brotli no disponible, usando contenido tal como está")
            except Exception as e:
                logger.warning(f"Error en decodificación manual: {e}")
        
        # Extraer sitekey del Turnstile
        sitekey = "0x4AAAAAAADWGgqdTg_SY-mN"  # Del HTML proporcionado
        
        # Extraer authenticity_token del formulario
        import re
        
        # Solo procesar HTML sin guardar archivos de debug
        
        # Buscar token con diferentes patrones
        auth_token_match = re.search(r'name="authenticity_key"\s+value="([^"]+)"', html_content)
        if not auth_token_match:
            auth_token_match = re.search(r'authenticity_key.*?value="([^"]+)"', html_content)
        if not auth_token_match:
            auth_token_match = re.search(r'authenticity_token.*?value="([^"]+)"', html_content)
        if not auth_token_match:
            auth_token_match = re.search(r'csrf-token.*?content="([^"]+)"', html_content)
            
        authenticity_token = auth_token_match.group(1) if auth_token_match else None
        
        # También buscar csrf-token en meta tag
        csrf_token_match = re.search(r'name="csrf-token"\s+content="([^"]+)"', html_content)
        csrf_token = csrf_token_match.group(1) if csrf_token_match else None
        
        logger.info(f"CSRF token encontrado: {csrf_token}")
        
        # Usar csrf-token si no tenemos authenticity_token
        if not authenticity_token and csrf_token:
            authenticity_token = csrf_token
        
        logger.info(f"Sitekey encontrado: {sitekey}")
        logger.info(f"Authenticity token encontrado: {authenticity_token}")
        
        return sitekey, authenticity_token, html_content

    def login(self):
        """
        Realiza el login con bypass de Cloudflare Turnstile
        """
        logger.info("Iniciando proceso de login...")
        
        # Obtener página de login
        sitekey, authenticity_token, html_content = self.get_login_page()
        
        if not sitekey or not authenticity_token:
            logger.error("No se pudieron extraer los datos necesarios del formulario")
            return False
            
        # Resolver Turnstile
        turnstile_token = self.solve_turnstile(sitekey, self.login_url)
        
        if not turnstile_token:
            logger.error("No se pudo resolver el Cloudflare Turnstile")
            return False
            
        # Preparar datos del formulario
        form_data = {
            "utf8": "✓",
            "authenticity_key": authenticity_token,
            "manager[email]": self.email,
            "manager[password]": self.password,
            "cf_turnstile_token": turnstile_token,
            "cf-turnstile-response": turnstile_token,
            "commit": "Ingresar"
        }
        
        # Headers adicionales para el POST
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': self.login_url,
            'Origin': self.base_url
        }
        
        # Logging del formulario que se va a enviar
        logger.info("Datos del formulario a enviar:")
        for key, value in form_data.items():
            if 'password' not in key.lower():
                logger.info(f"  {key}: {value}")
            else:
                logger.info(f"  {key}: [OCULTO]")
        
        # Realizar login
        print("\n🚀 Enviando formulario de login...")
        logger.info("Enviando formulario de login...")
        response = self.session.post(self.login_url, data=form_data, headers=headers, allow_redirects=False)
        
        logger.info(f"Status code: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        
        # Solo procesar respuesta sin guardar archivos de debug
        
        # Verificar resultado
        if response.status_code in [301, 302, 303]:
            print("🔄 Redirección detectada...")
            logger.info("Redirección detectada...")
            
            # Seguir redirección
            if 'Location' in response.headers:
                redirect_url = response.headers['Location']
                if not redirect_url.startswith('http'):
                    redirect_url = self.base_url + redirect_url
                    
                print(f"➡️  Siguiendo redirección a: {redirect_url}")
                logger.info(f"Siguiendo redirección a: {redirect_url}")
                final_response = self.session.get(redirect_url)
                
                logger.info(f"Status final: {final_response.status_code}")
                
                if final_response.status_code == 200:
                    print("🎉 ¡Acceso exitoso al panel de administración!")
                    logger.info("Acceso exitoso al panel de administración!")
                    return True
                else:
                    logger.error(f"Error en redirección final: {final_response.status_code}")
                    logger.error(f"Content: {final_response.text[:500]}...")
            else:
                logger.error("Redirección sin Location header")
                
        elif response.status_code == 200:
            # Verificar si hay errores en el formulario
            response_lower = response.text.lower()
            if ("error" in response_lower or "invalid" in response_lower or 
                "incorrect" in response_lower or "wrong" in response_lower or
                "email" in response_lower and "password" in response_lower):
                logger.error("Error en credenciales o formulario")
                logger.error(f"Response content: {response.text[:1000]}...")
            else:
                logger.info("Login posiblemente exitoso (200)")
                return True
                
        logger.error(f"Login fallido. Status code: {response.status_code}")
        logger.error(f"Response content: {response.text[:500]}...")
        return False

    def scrape_events(self):
        """
        Scraper principal para obtener eventos
        """
        if not self.login():
            logger.error("No se pudo realizar el login")
            return False
            
        logger.info("Iniciando scraping de eventos...")
        
        # Verificar que estamos realmente logueados probando diferentes URLs
        test_urls = [
            f"{self.base_url}/manage",
            f"{self.base_url}/manage/dashboard",
            f"{self.base_url}/manage/events",
            f"{self.base_url}/manage/home"
        ]
        
        for url in test_urls:
            logger.info(f"Probando acceso a: {url}")
            
            # Headers adicionales para mantener la sesión
            headers = {
                'Referer': self.login_url,
                'X-Requested-With': 'XMLHttpRequest'
            }
            
            response = self.session.get(url, headers=headers)
            
            logger.info(f"Status: {response.status_code}")
            logger.info(f"Cookies actuales: {dict(self.session.cookies)}")
            
            # Verificar si la respuesta contiene formulario de login
            if response.status_code == 200:
                content_lower = response.text.lower()
                
                # Buscar elementos específicos del formulario de login
                has_login_form = (
                    'id="manager_email"' in content_lower or 
                    'id="manager_password"' in content_lower or
                    'action="/manage/sign_in"' in content_lower or
                    'class="simple_form new_manager"' in content_lower
                )
                
                # Buscar indicadores de que estamos autenticados
                is_authenticated = (
                    'manage/application' in content_lower or
                    'eventos |' in content_lower or
                    'nav-wrapper' in content_lower or
                    '/manage/logout' in content_lower
                )
                
                if has_login_form and not is_authenticated:
                    logger.warning(f"URL {url} muestra formulario de login - no autenticado")
                    
                    # Solo registrar en log sin guardar archivos
                    
                elif is_authenticated:
                    print(f"✅ URL {url} - AUTENTICADO EXITOSAMENTE!")
                    logger.info(f"URL {url} - AUTENTICADO EXITOSAMENTE!")
                    
                    # Solo procesar contenido sin guardar archivos HTML
                    
                    print("🔍 Extrayendo datos de eventos...")
                    # Buscar enlaces o datos de eventos
                    self.extract_events_data(response.text)
                    
                    return True
                    
                else:
                    logger.info(f"URL {url} - estado desconocido (ni login ni autenticado claramente)")
            elif response.status_code in [301, 302, 303]:
                logger.info(f"Redirección desde {url} a: {response.headers.get('Location')}")
        
        logger.error("No se pudo acceder a ninguna área autenticada")
        return False
    
    def extract_events_data(self, html_content):
        """
        Extrae datos de eventos del HTML del panel de administración
        """
        logger.info("Extrayendo datos de eventos...")
        
        # Usar BeautifulSoup para parsear el HTML
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Buscar links, tablas, o elementos que contengan datos de eventos
            events_data = []
            
            # Buscar enlaces que contengan "event", "evento", etc.
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                if any(word in href.lower() for word in ['event', 'evento', 'show']) or \
                   any(word in text.lower() for word in ['event', 'evento', 'show']):
                    events_data.append({
                        'type': 'link',
                        'href': href,
                        'text': text
                    })
            
            # Buscar tablas que puedan contener datos de eventos
            tables = soup.find_all('table')
            for i, table in enumerate(tables):
                rows = table.find_all('tr')
                if len(rows) > 1:  # Tiene header y al menos una fila de datos
                    table_data = []
                    for row in rows:
                        cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                        if cells:
                            table_data.append(cells)
                    
                    if table_data:
                        events_data.append({
                            'type': 'table',
                            'table_index': i,
                            'data': table_data
                        })
            
            if events_data:
                print(f"🎊 ¡Encontrados {len(events_data)} elementos de eventos!")
                logger.info(f"Encontrados {len(events_data)} elementos de eventos")
                filename = self.save_data(events_data, "ticketea_events_data")
                print(f"💾 Datos guardados en: {filename}")
            else:
                print("⚠️  No se encontraron datos de eventos en el HTML")
                logger.warning("No se encontraron datos de eventos en el HTML")
                
        except ImportError:
            logger.error("BeautifulSoup no está disponible")
        except Exception as e:
            logger.error(f"Error extrayendo datos de eventos: {e}")

    def save_data(self, data, filename_prefix="ticketea_data"):
        """
        Guarda los datos scrapeados en formato JSON
        """
        if not os.path.exists("jsonticketea"):
            os.makedirs("jsonticketea")
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"jsonticketea/{filename_prefix}_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Datos guardados en: {filename}")
        return filename

def main():
    """
    Función principal
    """
    print("="*60)
    print("🎫 TICKETEA SCRAPER CON CLOUDFLARE TURNSTILE BYPASS 🎫")
    print("="*60)
    print("🔐 API 2captcha configurada")
    print("👤 Credenciales: camila.halfon@daleplay.la")
    print("🌐 Sitio: ticketea.com.py")
    print("-"*60)
    
    logger.info("=== Iniciando Ticketea Scraper ===")
    
    scraper = TicketeaScraper()
    
    try:
        success = scraper.scrape_events()
        
        if success:
            print("\n" + "="*60)
            print("🎉 ¡SCRAPING COMPLETADO EXITOSAMENTE! 🎉")
            print("="*60)
            logger.info("Scraping completado exitosamente!")
        else:
            print("\n" + "="*60)
            print("❌ ERROR DURANTE EL SCRAPING")
            print("="*60)
            logger.error("Error durante el scraping")
            
    except Exception as e:
        print(f"\n❌ Error inesperado: {str(e)}")
        logger.error(f"Error inesperado: {str(e)}", exc_info=True)
    
    print("\n🏁 Fin del proceso")
    logger.info("=== Fin del scraping ===")

if __name__ == "__main__":
    main()
