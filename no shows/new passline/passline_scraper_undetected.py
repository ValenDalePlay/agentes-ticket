#!/usr/bin/env python3
"""
Passline Scraper con Undetected Chrome
Scraper específico para pasar Cloudflare usando undetected-chromedriver
"""

import time
import json
import os
import random
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from twocaptcha import TwoCaptcha

class PasslineScraperUndetected:
    def __init__(self, email=None, password=None):
        self.driver = None
        self.wait = None
        self.login_url = "https://home.passline.com/login"
        self.email = email if email else "emilio@lauriaweb.com"
        self.password = password if password else "3nk5878651"
        self.captcha_api_key = "af9e3756d516cdb21be7f0d4d5e83f8b"
        self.solver = TwoCaptcha(self.captcha_api_key)
        self.account_name = self.email.split("@")[0]  # Para identificar la cuenta en logs
        
    def setup_driver(self):
        """Configura undetected Chrome driver para pasar Cloudflare"""
        try:
            print("🔧 Configurando undetected Chrome driver...")
            
            # Configuración específica para undetected-chromedriver
            options = uc.ChromeOptions()
            
            # Configuraciones básicas
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1366,768")
            
            # Configuraciones adicionales para parecer más humano
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-extensions")
            options.add_argument("--no-first-run")
            options.add_argument("--disable-default-apps")
            
            # User agent realista
            options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Configurar preferencias
            prefs = {
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0,
                "profile.managed_default_content_settings.images": 1
            }
            options.add_experimental_option("prefs", prefs)
            
            # Crear driver con undetected-chromedriver
            self.driver = uc.Chrome(
                options=options,
                version_main=139,  # Especificar versión de Chrome 139
                driver_executable_path=None,  # Auto-descargar
                browser_executable_path=None,  # Usar Chrome por defecto
                headless=False,  # Mostrar navegador
                use_subprocess=True,  # Usar subprocess para mayor stealth
                debug=False  # Sin debug
            )
            
            # Configurar timeouts
            self.driver.set_page_load_timeout(60)
            self.driver.implicitly_wait(10)
            self.wait = WebDriverWait(self.driver, 30)
            
            # Scripts adicionales anti-detección después de crear el driver
            self.driver.execute_script("""
                // Configurar navigator properties
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['es-419', 'es', 'en-US', 'en']
                });
                
                // Configurar plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => ({
                        length: 3,
                        0: {name: 'Chrome PDF Plugin'},
                        1: {name: 'Chrome PDF Viewer'},
                        2: {name: 'Native Client'}
                    })
                });
                
                // Configurar chrome object
                if (!window.chrome) {
                    window.chrome = {
                        runtime: {},
                        app: {isInstalled: false}
                    };
                }
                
                console.log('🔒 Undetected Chrome configurado');
            """)
            
            print("✅ Undetected Chrome driver configurado exitosamente")
            return True
            
        except Exception as e:
            print(f"❌ Error configurando undetected Chrome driver: {e}")
            return False
    
    def login(self):
        """Realiza el login en Passline con estrategia anti-Cloudflare"""
        try:
            print("🌐 Iniciando proceso de login...")
            
            # Estrategia 1: Navegar primero a la página principal
            print("🏠 Navegando a página principal...")
            self.driver.get("https://home.passline.com/")
            
            # Esperar más tiempo para que Cloudflare procese
            print("⏳ Esperando validación de Cloudflare (1 segundos)...")
            time.sleep(1)
            
            # Verificar si pasamos Cloudflare
            current_url = self.driver.current_url
            page_title = self.driver.title.lower()
            
            if "cloudflare" in page_title or "checking" in page_title:
                print("🔄 Cloudflare detectado, esperando más tiempo...")
                time.sleep(30)
            
            print(f"📍 URL actual: {current_url}")
            print(f"📄 Título: {self.driver.title}")
            
            # Ahora ir al login
            print("🔐 Navegando al login...")
            self.driver.get(self.login_url)
            time.sleep(10)
            
            # Verificar si llegamos al login
            try:
                # Buscar el formulario de login
                login_form = self.wait.until(
                    EC.presence_of_element_located((By.ID, "form-login"))
                )
                print("✅ Formulario de login encontrado")
            except TimeoutException:
                print("❌ No se encontró el formulario de login")
                print(f"📍 URL actual: {self.driver.current_url}")
                print(f"📄 Título actual: {self.driver.title}")
                
                # Guardar página para debug
                with open("debug_page.html", "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)
                print("💾 Página guardada en debug_page.html para análisis")
                return False
            
            # Buscar campo email con comportamiento humano
            print("📧 Buscando campo de email...")
            try:
                email_field = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "emaillogin"))
                )
                
                # Simular comportamiento humano
                self.human_type(email_field, self.email)
                print(f"✅ Email ingresado: {self.email}")
                time.sleep(1 + random.random())
                
            except TimeoutException:
                print("❌ No se encontró el campo de email")
                return False
            
            # Buscar campo password
            print("🔒 Buscando campo de contraseña...")
            try:
                password_field = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "passwordlogin"))
                )
                
                # Simular comportamiento humano
                self.human_type(password_field, self.password)
                print("✅ Contraseña ingresada")
                time.sleep(1.5 + random.random())
                
            except TimeoutException:
                print("❌ No se encontró el campo de contraseña")
                return False
            
            # Resolver reCAPTCHA automáticamente
            print("\n🤖 Resolviendo reCAPTCHA automáticamente con 2captcha...")
            if not self.solve_recaptcha():
                print("❌ Error resolviendo reCAPTCHA, continuando sin resolver...")
                # Pausa de respaldo por si falla 2captcha
                print("⏳ Esperando 10 segundos como respaldo...")
                time.sleep(10)
            
            # Buscar y hacer click en el botón de login
            print("🔍 Buscando botón de login...")
            try:
                login_button = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "btnLogin"))
                )
                
                # Click con delay humano
                time.sleep(1)
                login_button.click()
                print("✅ Botón de login clickeado")
                
            except TimeoutException:
                print("❌ No se encontró el botón de login")
                return False
            
            # Esperar a que se procese el login
            time.sleep(8)
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            if "login" not in current_url:
                print("✅ Login exitoso!")
                print(f"📍 Nueva URL: {current_url}")
                
                # Buscar y hacer click en el botón de Argentina
                return self.select_argentina()
            else:
                print("❌ Login falló - aún en página de login")
                print(f"📍 URL actual: {current_url}")
                print("🔄 Cerrando navegador automáticamente...")
                return False
                
        except Exception as e:
            print(f"❌ Error durante login: {e}")
            return False
    
    def select_argentina(self):
        """Busca y hace click en el botón de Argentina"""
        try:
            print("🇦🇷 Buscando botón de Argentina...")
            
            # Esperar a que cargue la página de selección de país
            time.sleep(3)
            
            # Buscar el botón de Argentina por diferentes métodos
            argentina_button = None
            
            # Método 1: Por data-slug
            try:
                argentina_button = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-slug="argentina"]'))
                )
                print("✅ Botón de Argentina encontrado por data-slug")
            except TimeoutException:
                pass
            
            # Método 2: Por clase y texto
            if not argentina_button:
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button.btn-country-init')
                    for button in buttons:
                        if "Argentina" in button.text or "argentina" in button.get_attribute("data-slug"):
                            argentina_button = button
                            print("✅ Botón de Argentina encontrado por texto")
                            break
                except Exception:
                    pass
            
            # Método 3: Por texto directo
            if not argentina_button:
                try:
                    argentina_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Argentina')]")
                    print("✅ Botón de Argentina encontrado por XPath")
                except Exception:
                    pass
            
            if argentina_button:
                # Hacer click con comportamiento humano
                print("🖱️  Haciendo click en botón de Argentina...")
                time.sleep(1 + random.random())
                
                # Scroll al elemento si es necesario
                self.driver.execute_script("arguments[0].scrollIntoView(true);", argentina_button)
                time.sleep(0.5)
                
                # Click con JavaScript como respaldo
                try:
                    argentina_button.click()
                except Exception:
                    self.driver.execute_script("arguments[0].click();", argentina_button)
                
                print("✅ Click en botón de Argentina realizado")
                
                # Esperar a que se procese la selección
                time.sleep(5)
                
                # Verificar si la selección fue exitosa
                current_url = self.driver.current_url
                print(f"📍 Nueva URL después de seleccionar Argentina: {current_url}")
                
                # Navegar al perfil e Informes Ventas Externos
                if self.navigate_to_informes_ventas():
                    # Procesar todos los enlaces de "Ventas por Dia"
                    return self.process_ventas_por_dia()
                return False
                
            else:
                print("❌ No se encontró el botón de Argentina")
                
                # Guardar página para debug
                with open("debug_country_selection.html", "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)
                print("💾 Página guardada en debug_country_selection.html para análisis")
                
                return False
                
        except Exception as e:
            print(f"❌ Error seleccionando Argentina: {e}")
            return False
    
    def select_argentina_only(self):
        """Selecciona Argentina sin navegar a otros lugares"""
        try:
            print("🇦🇷 Seleccionando Argentina...")
            
            # Buscar el botón de Argentina por diferentes métodos
            argentina_button = None
            
            # Método 1: Por data-slug
            try:
                argentina_button = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-slug="argentina"]'))
                )
                print("✅ Botón de Argentina encontrado por data-slug")
            except TimeoutException:
                pass
            
            # Método 2: Por clase y texto
            if not argentina_button:
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button.btn-country-init')
                    for button in buttons:
                        if "Argentina" in button.text or "argentina" in button.get_attribute("data-slug"):
                            argentina_button = button
                            print("✅ Botón de Argentina encontrado por texto")
                            break
                except Exception:
                    pass
            
            # Método 3: Por texto directo
            if not argentina_button:
                try:
                    argentina_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Argentina')]")
                    print("✅ Botón de Argentina encontrado por XPath")
                except Exception:
                    pass
            
            if argentina_button:
                # Hacer click con comportamiento humano
                print("🖱️  Haciendo click en botón de Argentina...")
                time.sleep(1 + random.random())
                
                # Scroll al elemento si es necesario
                self.driver.execute_script("arguments[0].scrollIntoView(true);", argentina_button)
                time.sleep(0.5)
                
                # Click con JavaScript como respaldo
                try:
                    argentina_button.click()
                except Exception:
                    self.driver.execute_script("arguments[0].click();", argentina_button)
                
                print("✅ Click en botón de Argentina realizado")
                time.sleep(5)
                return True
            else:
                print("❌ No se encontró el botón de Argentina")
                return False
                
        except Exception as e:
            print(f"❌ Error seleccionando Argentina: {e}")
            return False
    
    def navigate_to_informes_ventas(self):
        """Navega directamente a la página de Informes Ventas Externos"""
        try:
            print("📊 Navegando directamente a Informes Ventas Externos...")
            
            # Esperar a que cargue la página principal
            time.sleep(3)
            
            # Verificar si necesita seleccionar Argentina nuevamente
            current_url = self.driver.current_url
            print(f"📍 URL actual: {current_url}")
            
            # Verificar si hay botón de Argentina visible (indicando que necesita seleccionar país)
            argentina_visible = False
            try:
                # Buscar si hay botón de Argentina en la página
                argentina_button = self.driver.find_element(By.CSS_SELECTOR, 'button[data-slug="argentina"]')
                if argentina_button.is_displayed():
                    argentina_visible = True
                    print("🇦🇷 Detectado botón de Argentina visible - necesita seleccionar país")
            except:
                # También verificar por URL si está en página de selección
                if "home.passline.com" in current_url and "/home" not in current_url:
                    argentina_visible = True
                    print("🇦🇷 Detectado por URL que necesita seleccionar Argentina")
            
            if argentina_visible:
                print("🇦🇷 Seleccionando Argentina antes de navegar...")
                if not self.select_argentina_only():
                    return False
                time.sleep(3)
            
            # Navegar directamente a la página de informes
            informes_url = "https://www.passline.com/mis-informes-ventas.php"
            print(f"🔗 Navegando directamente a: {informes_url}")
            
            self.driver.get(informes_url)
            time.sleep(5)
            
            # Verificar si llegamos a la página correcta
            final_url = self.driver.current_url
            print(f"📍 URL final: {final_url}")
            
            if "mis-informes-ventas" in final_url:
                print("✅ Navegación directa a Informes Ventas Externos exitosa")
                return True
            else:
                print("⚠️  La URL final no contiene 'mis-informes-ventas', pero continuando...")
                return True
                
        except Exception as e:
            print(f"❌ Error navegando directamente a Informes Ventas Externos: {e}")
            return False
    
    def is_future_or_today_date(self, date_str):
        """Verifica si una fecha es igual o posterior a hoy"""
        try:
            # Parsear la fecha del formato "12-09-2025 21:00:00"
            event_date = datetime.strptime(date_str.strip(), "%d-%m-%Y %H:%M:%S")
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            return event_date.date() >= today.date()
        except Exception as e:
            print(f"⚠️  Error parseando fecha '{date_str}': {e}")
            return False
    
    def get_filtered_ventas_urls(self):
        """Extrae URLs de 'Ventas por Dia' filtradas por fecha"""
        try:
            print("📅 Filtrando eventos por fecha...")
            
            # Buscar todas las filas de la tabla
            table_rows = self.driver.find_elements(By.XPATH, "//table[@class='tabla colapsar-tabla']//tr[position()>1]")
            
            if not table_rows:
                print("❌ No se encontraron filas en la tabla de eventos")
                return []
            
            print(f"🔍 Analizando {len(table_rows)} eventos...")
            
            filtered_urls = []
            today = datetime.now().date()
            
            for i, row in enumerate(table_rows):
                try:
                    # Extraer la fecha (primera celda)
                    date_cell = row.find_element(By.XPATH, "./td[1]")
                    date_str = date_cell.text.strip()
                    
                    # Extraer el nombre del evento (tercera celda)
                    event_cell = row.find_element(By.XPATH, "./td[3]")
                    event_name = event_cell.text.strip()
                    
                    # Extraer el enlace "Ventas por Dia" (quinta celda)
                    ventas_link = row.find_element(By.XPATH, "./td[5]//a[contains(text(), 'Ventas por Dia')]")
                    ventas_url = ventas_link.get_attribute("href")
                    
                    # Verificar si la fecha es igual o posterior a hoy
                    if self.is_future_or_today_date(date_str):
                        filtered_urls.append({
                            "url": ventas_url,
                            "text": f"Ventas por Dia - {event_name}",
                            "date": date_str,
                            "event_name": event_name
                        })
                        print(f"✅ Evento incluido: {event_name} ({date_str})")
                    else:
                        print(f"⏭️  Evento omitido (fecha pasada): {event_name} ({date_str})")
                        
                except Exception as e:
                    print(f"⚠️  Error procesando fila {i+1}: {e}")
                    continue
            
            print(f"\n📊 Resumen del filtrado:")
            print(f"   • Total eventos encontrados: {len(table_rows)}")
            print(f"   • Eventos con fecha >= hoy: {len(filtered_urls)}")
            print(f"   • Fecha de referencia: {today.strftime('%d-%m-%Y')}")
            
            return filtered_urls
            
        except Exception as e:
            print(f"❌ Error filtrando eventos por fecha: {e}")
            return []

    def process_ventas_por_dia(self):
        """Encuentra y procesa todos los enlaces de 'Ventas por Dia' filtrados por fecha"""
        try:
            print("📊 Procesando enlaces de 'Ventas por Dia'...")
            
            # Esperar a que cargue completamente la página
            time.sleep(3)
            
            # Guardar URL de la página de informes para volver
            informes_url = self.driver.current_url
            print(f"📍 URL base de informes: {informes_url}")
            
            # Obtener URLs filtradas por fecha
            ventas_urls = self.get_filtered_ventas_urls()
            
            if not ventas_urls:
                print("❌ No se encontraron eventos con fechas futuras o de hoy")
                return False
            
            print(f"✅ Se procesarán {len(ventas_urls)} eventos con fechas válidas")
            
            # Almacenar datos de todos los eventos
            all_ventas_data = {
                "timestamp": datetime.now().isoformat(),
                "user_email": self.email,
                "total_eventos": len(ventas_urls),
                "fecha_filtro": datetime.now().date().isoformat(),
                "eventos_data": []
            }
            
            # Procesar cada URL directamente
            for i, link_data in enumerate(ventas_urls):
                try:
                    print(f"\n🔍 Procesando evento {i+1}/{len(ventas_urls)}...")
                    
                    link_url = link_data["url"]
                    link_text = link_data["text"]
                    event_date = link_data["date"]
                    event_name = link_data["event_name"]
                    
                    print(f"📅 Fecha: {event_date}")
                    print(f"🎭 Evento: {event_name}")
                    print(f"📎 Enlace: {link_url}")
                    
                    # Navegar directamente a la URL del evento
                    self.driver.get(link_url)
                    time.sleep(5)
                    
                    # Extraer datos del evento
                    evento_data = self.extract_evento_ventas_data()
                    
                    if evento_data:
                        evento_data["link_original"] = link_url
                        evento_data["link_text"] = link_text
                        evento_data["evento_numero"] = i + 1
                        evento_data["fecha_evento"] = event_date
                        evento_data["nombre_evento"] = event_name
                        all_ventas_data["eventos_data"].append(evento_data)
                        print(f"✅ Datos extraídos para evento {i+1}")
                    else:
                        print(f"⚠️  No se pudieron extraer datos para evento {i+1}")
                    
                    # Pequeña pausa entre eventos
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"❌ Error procesando evento {i+1}: {e}")
                    continue
            
            # Volver a la página de informes al final
            try:
                print(f"\n🔄 Regresando a página de informes: {informes_url}")
                self.driver.get(informes_url)
                time.sleep(3)
            except Exception as e:
                print(f"⚠️  Error regresando a página de informes: {e}")
            
            # Guardar todos los datos
            self.save_all_ventas_data(all_ventas_data)
            
            print(f"\n✅ Procesamiento completado: {len(all_ventas_data['eventos_data'])} eventos procesados exitosamente")
            return True
            
        except Exception as e:
            print(f"❌ Error general procesando ventas por día: {e}")
            return False
    
    def extract_evento_ventas_data(self):
        """Extrae datos detallados de una página de ventas por día de un evento"""
        try:
            print("📋 Extrayendo datos del evento...")
            
            # Esperar a que cargue la página
            time.sleep(3)
            
            evento_data = {
                "timestamp": datetime.now().isoformat(),
                "url": self.driver.current_url,
                "titulo_pagina": self.driver.title,
                "evento_nombre": "",
                "resumen_ventas": {},
                "venta_etickets": [],
                "venta_productos": [],
                "ventas_boleteria": [],
                "ventas_planimetria": [],
                "total_venta": ""
            }
            
            # Extraer nombre del evento
            try:
                evento_element = self.driver.find_element(By.XPATH, "//h3[contains(text(), 'Evento:')]")
                evento_data["evento_nombre"] = evento_element.text.replace("Evento: ", "").strip()
                print(f"🎫 Evento: {evento_data['evento_nombre']}")
            except:
                print("⚠️  No se pudo extraer el nombre del evento")
            
            # Extraer tabla de E-Tickets
            try:
                print("📊 Extrayendo datos de E-Tickets...")
                etickets_table = self.driver.find_element(By.XPATH, "//h3[text()='Venta de E-Tickets']/following-sibling::table")
                etickets_rows = etickets_table.find_elements(By.TAG_NAME, "tr")[1:]  # Saltar header
                
                for row in etickets_rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 6:
                        row_data = {
                            "fecha_tipo": cells[0].text.strip(),
                            "web": cells[1].text.strip(),
                            "invitaciones": cells[2].text.strip(),
                            "efectivo": cells[3].text.strip(),
                            "total_vendidos": cells[4].text.strip(),
                            "monto_total": cells[5].text.strip()
                        }
                        evento_data["venta_etickets"].append(row_data)
                
                print(f"✅ Extraídas {len(evento_data['venta_etickets'])} filas de E-Tickets")
            except Exception as e:
                print(f"⚠️  Error extrayendo E-Tickets: {e}")
            
            # Extraer tabla de Productos
            try:
                print("📦 Extrayendo datos de Productos...")
                productos_table = self.driver.find_element(By.XPATH, "//h3[text()='Venta de Productos']/following-sibling::table")
                productos_rows = productos_table.find_elements(By.TAG_NAME, "tr")[1:]  # Saltar header
                
                for row in productos_rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 3:
                        row_data = {
                            "tipo_producto": cells[0].text.strip(),
                            "vendidos": cells[1].text.strip(),
                            "monto": cells[2].text.strip()
                        }
                        evento_data["venta_productos"].append(row_data)
                
                print(f"✅ Extraídas {len(evento_data['venta_productos'])} filas de Productos")
            except Exception as e:
                print(f"⚠️  Error extrayendo Productos: {e}")
            
            # Extraer tabla de Boletería
            try:
                print("🎫 Extrayendo datos de Boletería...")
                boleteria_table = self.driver.find_element(By.XPATH, "//h3[text()='Ventas Boletería']/following-sibling::table")
                boleteria_rows = boleteria_table.find_elements(By.TAG_NAME, "tr")[1:]  # Saltar header
                
                for row in boleteria_rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 9:
                        row_data = {
                            "fecha": cells[0].text.strip(),
                            "tarjeta": cells[1].text.strip(),
                            "monto_tarjeta": cells[2].text.strip(),
                            "efectivo": cells[3].text.strip(),
                            "monto_efectivo": cells[4].text.strip(),
                            "transferencia": cells[5].text.strip(),
                            "monto_transferencia": cells[6].text.strip(),
                            "total_vendidos": cells[7].text.strip(),
                            "monto_total": cells[8].text.strip()
                        }
                        evento_data["ventas_boleteria"].append(row_data)
                
                print(f"✅ Extraídas {len(evento_data['ventas_boleteria'])} filas de Boletería")
            except Exception as e:
                print(f"⚠️  Error extrayendo Boletería: {e}")
            
            # Extraer tabla de Planimetría
            try:
                print("📐 Extrayendo datos de Planimetría...")
                planimetria_table = self.driver.find_element(By.XPATH, "//h3[text()='Ventas Planimetria por Boletería']/following-sibling::table")
                planimetria_rows = planimetria_table.find_elements(By.TAG_NAME, "tr")[1:]  # Saltar header
                
                for row in planimetria_rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 5:
                        row_data = {
                            "fecha": cells[0].text.strip(),
                            "tarjeta": cells[1].text.strip(),
                            "efectivo": cells[2].text.strip(),
                            "total_vendidos": cells[3].text.strip(),
                            "monto_total": cells[4].text.strip()
                        }
                        evento_data["ventas_planimetria"].append(row_data)
                
                print(f"✅ Extraídas {len(evento_data['ventas_planimetria'])} filas de Planimetría")
            except Exception as e:
                print(f"⚠️  Error extrayendo Planimetría: {e}")
            
            # Extraer total de venta
            try:
                total_element = self.driver.find_element(By.XPATH, "//h3[contains(text(), 'Total Venta:')]")
                evento_data["total_venta"] = total_element.text.replace("Total Venta:", "").strip()
                print(f"💰 Total Venta: {evento_data['total_venta']}")
            except Exception as e:
                print(f"⚠️  Error extrayendo Total Venta: {e}")
            
            return evento_data
            
        except Exception as e:
            print(f"❌ Error extrayendo datos del evento: {e}")
            return None
    
    def save_all_ventas_data(self, all_data):
        """Guarda todos los datos de ventas en un archivo JSON"""
        try:
            # Crear carpeta json si no existe
            json_dir = "jsonpassline"
            os.makedirs(json_dir, exist_ok=True)
            
            # Generar nombre de archivo con identificador de cuenta
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"passline_ventas_{self.account_name}_{timestamp}.json"
            filepath = os.path.join(json_dir, filename)
            
            # Guardar datos
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 Datos completos de ventas guardados en: {filepath}")
            return True
            
        except Exception as e:
            print(f"❌ Error guardando datos de ventas: {e}")
            return False
    
    def solve_recaptcha(self):
        """Resuelve el reCAPTCHA usando 2captcha"""
        try:
            print("🤖 Resolviendo reCAPTCHA con 2captcha...")
            
            # Buscar el reCAPTCHA en la página
            try:
                # Buscar el site key del reCAPTCHA
                recaptcha_element = self.driver.find_element(By.CLASS_NAME, "g-recaptcha")
                site_key = recaptcha_element.get_attribute("data-sitekey")
                print(f"🔑 Site key encontrado: {site_key}")
            except NoSuchElementException:
                print("❌ No se encontró el elemento reCAPTCHA")
                return False
            
            # Obtener la URL actual
            page_url = self.driver.current_url
            print(f"🌐 Resolviendo para URL: {page_url}")
            
            # Resolver el reCAPTCHA con 2captcha
            print("⏳ Enviando reCAPTCHA a 2captcha para resolución...")
            try:
                result = self.solver.recaptcha(
                    sitekey=site_key,
                    url=page_url
                )
                
                captcha_response = result['code']
                print("✅ reCAPTCHA resuelto exitosamente!")
                print(f"🎯 Respuesta obtenida (primeros 50 chars): {captcha_response[:50]}...")
                
            except Exception as e:
                print(f"❌ Error resolviendo reCAPTCHA: {e}")
                return False
            
            # Insertar la respuesta del reCAPTCHA en el formulario
            try:
                # Buscar el textarea de respuesta del reCAPTCHA
                response_element = self.driver.find_element(By.NAME, "g-recaptcha-response")
                
                # Hacer visible el textarea (normalmente está oculto)
                self.driver.execute_script("arguments[0].style.display = 'block';", response_element)
                
                # Insertar la respuesta
                self.driver.execute_script(f"arguments[0].value = '{captcha_response}';", response_element)
                
                # Ejecutar callback si existe
                self.driver.execute_script("""
                    if (typeof verifyCaptcha === 'function') {
                        verifyCaptcha();
                    }
                    if (typeof grecaptcha !== 'undefined') {
                        grecaptcha.getResponse = function() { return arguments[0]; }.bind(null, arguments[0]);
                    }
                """, captcha_response)
                
                print("✅ Respuesta del reCAPTCHA insertada en el formulario")
                time.sleep(2)
                return True
                
            except Exception as e:
                print(f"❌ Error insertando respuesta del reCAPTCHA: {e}")
                return False
                
        except Exception as e:
            print(f"❌ Error general resolviendo reCAPTCHA: {e}")
            return False
    
    def human_type(self, element, text):
        """Simula escritura humana con delays aleatorios"""
        element.click()
        time.sleep(0.3 + random.random() * 0.3)
        element.clear()
        time.sleep(0.2 + random.random() * 0.2)
        
        for char in text:
            element.send_keys(char)
            # Delay aleatorio entre 0.05 y 0.15 segundos por carácter
            time.sleep(0.05 + random.random() * 0.1)
    
    def scrape_dashboard(self):
        """Extrae información de Informes Ventas Externos"""
        try:
            print("📊 Extrayendo información de Informes Ventas Externos...")
            
            # Esperar a que cargue la página
            time.sleep(5)
            
            # Obtener información básica
            page_title = self.driver.title
            current_url = self.driver.current_url
            
            informes_data = {
                "timestamp": datetime.now().isoformat(),
                "page_title": page_title,
                "current_url": current_url,
                "user_email": self.email,
                "informes_ventas": [],
                "ventas_externas": [],
                "reportes": [],
                "page_source_length": len(self.driver.page_source)
            }
            
            # Buscar informes y ventas externas
            try:
                # Buscar elementos de informes/reportes
                informe_elements = self.driver.find_elements(By.CSS_SELECTOR, "[class*='informe'], [class*='reporte'], [class*='venta'], [class*='tabla'], .table, .card")
                print(f"📋 Encontrados {len(informe_elements)} elementos de informes/reportes")
                
                informes_found = []
                for element in informe_elements:
                    try:
                        informe_info = {
                            "text": element.text.strip(),
                            "html": element.get_attribute("outerHTML")[:500] if element.get_attribute("outerHTML") else "",
                            "classes": element.get_attribute("class"),
                            "tag": element.tag_name
                        }
                        
                        if informe_info["text"] and len(informe_info["text"]) > 10:
                            informes_found.append(informe_info)
                            
                    except Exception as e:
                        continue
                
                informes_data["informes_ventas"] = informes_found
                
                # Buscar información específica en tablas
                tables = self.driver.find_elements(By.TAG_NAME, "table")
                table_data = []
                for table in tables:
                    try:
                        table_text = table.text.strip()
                        if table_text:
                            # Extraer información de filas de tabla
                            rows = table.find_elements(By.TAG_NAME, "tr")
                            table_info = {
                                "full_text": table_text,
                                "rows_count": len(rows),
                                "rows_data": []
                            }
                            
                            for row in rows[:10]:  # Limitar a 10 filas
                                try:
                                    cells = row.find_elements(By.TAG_NAME, "td")
                                    if cells:
                                        row_data = [cell.text.strip() for cell in cells]
                                        table_info["rows_data"].append(row_data)
                                except:
                                    continue
                            
                            table_data.append(table_info)
                    except:
                        continue
                
                informes_data["tables_data"] = table_data
                
                # Buscar enlaces de descarga, exportar, PDF, etc.
                download_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='download'], a[href*='export'], a[href*='pdf'], a[href*='excel'], a[href*='csv'], a[href*='informe']")
                links_found = []
                for link in download_links:
                    try:
                        link_info = {
                            "text": link.text.strip(),
                            "href": link.get_attribute("href"),
                            "title": link.get_attribute("title")
                        }
                        links_found.append(link_info)
                    except:
                        continue
                
                informes_data["download_links"] = links_found
                
                # Buscar formularios o filtros
                forms = self.driver.find_elements(By.TAG_NAME, "form")
                form_data = []
                for form in forms:
                    try:
                        form_info = {
                            "action": form.get_attribute("action"),
                            "method": form.get_attribute("method"),
                            "inputs": []
                        }
                        
                        inputs = form.find_elements(By.TAG_NAME, "input")
                        for input_elem in inputs:
                            try:
                                input_info = {
                                    "name": input_elem.get_attribute("name"),
                                    "type": input_elem.get_attribute("type"),
                                    "value": input_elem.get_attribute("value")
                                }
                                form_info["inputs"].append(input_info)
                            except:
                                continue
                        
                        form_data.append(form_info)
                    except:
                        continue
                
                informes_data["forms_data"] = form_data
                
                print(f"✅ Extraídos {len(informes_found)} elementos de informes")
                print(f"✅ Extraídas {len(table_data)} tablas")
                print(f"✅ Extraídos {len(links_found)} enlaces")
                print(f"✅ Extraídos {len(form_data)} formularios")
                
            except Exception as e:
                print(f"⚠️  Error extrayendo elementos específicos: {e}")
            
            return informes_data
            
        except Exception as e:
            print(f"❌ Error extrayendo información de Informes Ventas Externos: {e}")
            return None
    
    def save_data(self, data, filename=None):
        """Guarda los datos extraídos en un archivo JSON"""
        if not data:
            print("❌ No hay datos para guardar")
            return False
            
        try:
            # Crear carpeta json si no existe
            json_dir = "jsonpassline"
            os.makedirs(json_dir, exist_ok=True)
            
            # Generar nombre de archivo si no se proporciona
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"passline_undetected_data_{timestamp}.json"
            
            filepath = os.path.join(json_dir, filename)
            
            # Guardar datos
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 Datos guardados en: {filepath}")
            return True
            
        except Exception as e:
            print(f"❌ Error guardando datos: {e}")
            return False
    
    def run(self):
        """Ejecuta el scraper completo para una cuenta"""
        try:
            print(f"🚀 Iniciando Passline Scraper para: {self.email}")
            
            # Configurar driver
            if not self.setup_driver():
                return False
            
            # Realizar login y procesamiento completo
            login_success = self.login()
            
            if login_success:
                print(f"✅ Scraper completado exitosamente para {self.account_name}!")
                print("🔄 Cerrando navegador automáticamente en 3 segundos...")
                time.sleep(3)
            else:
                print(f"❌ Login falló para {self.account_name}, cerrando navegador automáticamente")
                time.sleep(2)
            
            # Cerrar navegador
            if self.driver:
                self.driver.quit()
            return login_success
            
        except Exception as e:
            print(f"❌ Error general en scraper para {self.account_name}: {e}")
            return False

def run_multiple_accounts():
    """Ejecuta el scraper para múltiples cuentas"""
    print("🚀 Iniciando Passline Scraper Multi-Cuenta...")
    print("=" * 60)
    
    # Definir las cuentas a procesar (empezando con Camila)
    accounts = [
        {
            "email": "camila.halfon@daleplay.la", 
            "password": "ElSalvador5863",
            "name": "Camila"
        },
        {
            "email": "emilio@lauriaweb.com",
            "password": "3nk5878651",
            "name": "Emilio"
        }
    ]
    
    results = {}
    
    for i, account in enumerate(accounts, 1):
        print(f"\n🔄 Procesando cuenta {i}/{len(accounts)}: {account['name']} ({account['email']})")
        print("-" * 60)
        
        try:
            # Crear instancia del scraper para esta cuenta
            scraper = PasslineScraperUndetected(
                email=account['email'],
                password=account['password']
            )
            
            # Ejecutar scraper para esta cuenta
            success = scraper.run()
            results[account['email']] = success
            
            # Pausa entre cuentas
            if i < len(accounts):
                print(f"\n⏳ Esperando 10 segundos antes de procesar la siguiente cuenta...")
                time.sleep(10)
                
        except Exception as e:
            print(f"❌ Error procesando cuenta {account['name']}: {e}")
            results[account['email']] = False
    
    # Mostrar resumen final
    print("\n" + "=" * 60)
    print("📊 RESUMEN FINAL DE PROCESAMIENTO")
    print("=" * 60)
    
    successful = 0
    failed = 0
    
    for email, success in results.items():
        status = "✅ EXITOSO" if success else "❌ FALLÓ"
        account_name = email.split("@")[0]
        print(f"  {account_name} ({email}): {status}")
        
        if success:
            successful += 1
        else:
            failed += 1
    
    print(f"\n📈 Estadísticas:")
    print(f"  • Cuentas procesadas exitosamente: {successful}")
    print(f"  • Cuentas que fallaron: {failed}")
    print(f"  • Total de cuentas: {len(accounts)}")
    
    print(f"\n💾 Los archivos JSON se guardaron en la carpeta 'jsonpassline/'")
    print("🎉 Procesamiento multi-cuenta completado!")
    
    return results

def main():
    # Ejecutar para múltiples cuentas
    run_multiple_accounts()

if __name__ == "__main__":
    main()
