import time
import json
import re
import random
from datetime import datetime, timezone, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import logging
import pandas as pd
import psycopg2
from database_config import get_database_connection

class ProticketsScraper:
    def __init__(self, headless=True):
        self.driver = None
        self.headless = headless
        self.db_connection = None
        self.final_data = []
        self.setup_logging()
        self.setup_database_connection()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('protickets_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_database_connection(self):
        """Setup database connection"""
        try:
            self.db_connection = get_database_connection()
            if self.db_connection:
                self.logger.info("✅ Conexión exitosa! Hora actual: " + str(datetime.now(timezone.utc)))
            else:
                self.logger.error("❌ Error conectando a la base de datos")
        except Exception as e:
            self.logger.error(f"❌ Error en setup_database_connection: {e}")
        
    def setup_driver(self):
        """Setup Chrome driver with advanced bot evasion"""
        self.logger.info("Configurando driver de Chrome...")
        
        chrome_options = Options()
        
        # Modo headless
        if self.headless:
            chrome_options.add_argument("--headless")
            self.logger.info("Modo headless activado")
        
        # Configuración básica
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Evasión de bots avanzada
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # User agent realista
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
        
        # Configuración adicional
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-javascript")
        
        try:
            # Usar ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Scripts de evasión
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
            self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
            
            self.logger.info("Driver de Chrome configurado exitosamente")
            
        except Exception as e:
            self.logger.error(f"Error configurando ChromeDriverManager: {e}")
            self.logger.info("Intentando usar ChromeDriver del sistema...")
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.logger.info("Driver de Chrome configurado exitosamente")
        
    def login(self):
        """Login to protickets dashboard"""
        try:
            self.logger.info("Navigating to protickets dashboard...")
            self.driver.get("https://dashboard.protickets.com.ar/")
            
            # Wait for page to load
            time.sleep(3)
            
            # Find and fill email input
            self.logger.info("Looking for email input...")
            email_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "email"))
            )
            email_input.clear()
            email_input.send_keys('camila.halfon@daleplay.la')
            self.logger.info("Email entered successfully")
            
            # Find and fill password input
            self.logger.info("Looking for password input...")
            password_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "password"))
            )
            password_input.clear()
            password_input.send_keys('ElSalvador5863!')
            self.logger.info("Password entered successfully")
            
            # Find and click login button
            self.logger.info("Looking for login button...")
            login_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Log in')]"))
            )
            login_button.click()
            self.logger.info("Login button clicked successfully")
            
            return True
            
        except TimeoutException as e:
            self.logger.error(f"Timeout waiting for element: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error during login: {e}")
            return False
    
    def wait_for_events_icon(self):
        """Wait for events icon to appear and click it"""
        try:
            self.logger.info("Waiting for events icon to appear...")
            
            # Wait for the events icon div to appear
            events_icon = WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.app-icon.events-icon"))
            )
            
            self.logger.info("Events icon found, clicking it...")
            events_icon.click()
            
            # Wait 10 seconds as requested
            self.logger.info("Waiting 10 seconds after clicking events icon...")
            time.sleep(10)
            
            self.logger.info("Successfully navigated to events section")
            return True
            
        except TimeoutException as e:
            self.logger.error(f"Timeout waiting for events icon: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error clicking events icon: {e}")
            return False
    
    def extract_reports_links(self):
        """Extract href from anchor tags containing 'Reports' span within item-box divs"""
        try:
            self.logger.info("Searching for item-box divs with Reports links...")
            
            # Wait for page to load after navigation
            time.sleep(5)
            
            # Find all divs with class "item-box item-box-large"
            item_boxes = self.driver.find_elements(By.CSS_SELECTOR, "div.item-box.item-box-large")
            self.logger.info(f"Found {len(item_boxes)} item-box divs")
            
            reports_links = []
            
            for item_box in item_boxes:
                try:
                    # Look for anchor tags with span containing "Reports"
                    reports_anchors = item_box.find_elements(By.XPATH, ".//a[.//span[contains(text(), 'Reports')]]")
                    
                    for anchor in reports_anchors:
                        href = anchor.get_attribute('href')
                        if href:
                            reports_links.append(href)
                            self.logger.info(f"Found Reports link: {href}")
                
                except Exception as e:
                    self.logger.warning(f"Error processing item-box: {e}")
                    continue
            
            # Print the list of Reports links
            if reports_links:
                print("\n=== REPORTS LINKS FOUND ===")
                for i, link in enumerate(reports_links, 1):
                    print(f"{i}. {link}")
                print(f"\nTotal Reports links found: {len(reports_links)}")
            else:
                print("No Reports links found in item-box divs")
            
            return reports_links
            
        except Exception as e:
            self.logger.error(f"Error extracting Reports links: {e}")
            return []
    
    def extract_table_from_report(self, report_url, report_index):
        """Extract table data from a specific report page"""
        try:
            self.logger.info(f"Visiting report {report_index}: {report_url}")
            
            # Navigate to the report page
            self.driver.get(report_url)
            
            # Wait for page to load
            time.sleep(5)
            
            # First, extract text from divs with class "searchable-select mode-complete"
            self.logger.info("Looking for searchable-select divs...")
            searchable_selects = self.driver.find_elements(By.CSS_SELECTOR, "div.searchable-select.mode-complete")
            
            first_select_text = ""
            if searchable_selects:
                print(f"\n=== REPORT {report_index} SEARCHABLE SELECT DATA ===")
                print(f"URL: {report_url}")
                print(f"Found {len(searchable_selects)} searchable-select divs:")
                
                for i, select_div in enumerate(searchable_selects, 1):
                    try:
                        # Get the text content of the div
                        div_text = select_div.text.strip()
                        if div_text:
                            print(f"{i}. {div_text}")
                            # Store the first searchable-select text for filename
                            if i == 1:
                                first_select_text = div_text
                        else:
                            print(f"{i}. [Empty or no visible text]")
                    except Exception as e:
                        print(f"{i}. [Error extracting text: {e}]")
                
                print("-" * 50)
            else:
                print(f"\n=== REPORT {report_index} ===")
                print(f"URL: {report_url}")
                print("No searchable-select divs found")
                print("-" * 50)
            
            # Now look for div with class "table-content"
            self.logger.info("Looking for table-content div...")
            table_content = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.table-content"))
            )
            
            # Find the table within the table-content div
            self.logger.info("Looking for table element...")
            table = table_content.find_element(By.TAG_NAME, "table")
            
            # Extract table data
            self.logger.info("Extracting table data...")
            
            # Get headers
            headers = []
            header_cells = table.find_elements(By.CSS_SELECTOR, "thead th")
            for header in header_cells:
                headers.append(header.text.strip())
            
            # Get rows
            rows = []
            table_rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            for row in table_rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                row_data = []
                for cell in cells:
                    row_data.append(cell.text.strip())
                if row_data:  # Only add non-empty rows
                    rows.append(row_data)
            
            # Create DataFrame
            if headers and rows:
                df = pd.DataFrame(rows, columns=headers)
                
                # Display the DataFrame
                print(f"\n=== REPORT {report_index} TABLE DATA ===")
                print(f"Shape: {df.shape}")
                print("\nDataFrame:")
                print(df.to_string(index=False))
                print("\n" + "="*50)
                
                # Save table data to database
                if first_select_text:
                    self.save_single_event_to_database(df, first_select_text, report_index)
                else:
                    self.save_single_event_to_database(df, f"report_{report_index}", report_index)
                
                return df
            else:
                self.logger.warning(f"No table data found in report {report_index}")
                return None
                
        except TimeoutException as e:
            self.logger.error(f"Timeout waiting for table-content in report {report_index}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error extracting table from report {report_index}: {e}")
            return None
    
    def calculate_event_totals(self, df, title_text):
        """Calculate totals from ProTickets report data"""
        try:
            # ProTickets usa columnas específicas: CAPACITY, AVAILABILITY, SOLD, TOTAL AMOUNT
            if 'CAPACITY' in df.columns and 'SOLD' in df.columns:
                # Extraer valores de la primera fila (ProTickets tiene una fila por sector)
                row = df.iloc[0]
                
                # Capacidad total
                total_capacidad = int(str(row['CAPACITY']).replace(',', '').replace('.', ''))
                
                # Vendidos
                total_vendidos = int(str(row['SOLD']).replace(',', '').replace('.', ''))
                
                # Disponibles
                total_disponibles = int(str(row['AVAILABILITY']).replace(',', '').replace('.', ''))
                
                # Recaudación total
                total_recaudacion = 0
                if 'TOTAL AMOUNT' in df.columns:
                    amount_str = str(row['TOTAL AMOUNT']).replace('ARS', '').replace(',', '').replace('.', '').strip()
                    try:
                        total_recaudacion = int(amount_str)
                    except:
                        total_recaudacion = 0
                
                # Porcentaje de ocupación
                porcentaje_ocupacion = (total_vendidos / total_capacidad * 100) if total_capacidad > 0 else 0
                
                self.logger.info(f"📊 Totales calculados para evento:")
                self.logger.info(f"  📊 Capacidad: {total_capacidad}")
                self.logger.info(f"  🎫 Vendido: {total_vendidos}")
                self.logger.info(f"  🆓 Disponible: {total_disponibles}")
                self.logger.info(f"  💰 Recaudación: ${total_recaudacion}")
                self.logger.info(f"  📈 Ocupación: {porcentaje_ocupacion:.2f}%")
                
                return {
                    'capacidad_total': total_capacidad,
                    'vendido_total': total_vendidos,
                    'disponible_total': total_disponibles,
                    'recaudacion_total_ars': total_recaudacion,
                    'porcentaje_ocupacion': round(porcentaje_ocupacion, 2)
                }
            else:
                # Reporte genérico - intentar extraer números de las columnas
                total_capacidad = 0
                total_vendidos = 0
                total_disponibles = 0
                
                for col in df.columns:
                    if 'total' in col.lower() or 'cantidad' in col.lower():
                        try:
                            total_capacidad = df[col].astype(str).str.replace(',', '').str.replace('.', '').astype(int).sum()
                        except:
                            continue
                
                return {
                    'capacidad_total': total_capacidad,
                    'vendido_total': total_vendidos,
                    'disponible_total': total_disponibles,
                    'recaudacion_total_ars': 0,
                    'porcentaje_ocupacion': 0.0
                }
                
        except Exception as e:
            self.logger.error(f"Error calculando totales: {e}")
            return {
                'capacidad_total': 0,
                'vendido_total': 0,
                'disponible_total': 0,
                'recaudacion_total_ars': 0,
                'porcentaje_ocupacion': 0.0
            }
    
    def extract_event_info(self, title_text):
        """Extract event information from title"""
        try:
            # ProTickets titles format: "Evento (DD/MM/YYYY, HH:MM)"
            # Example: "Lali Tour 2025 (18/10/2025, 21:00)"
            # Example: "AIRBAG - EL CLUB DE LA PELEA (4/9/2025, 21:00)"
            
            # Extract date from parentheses
            date_match = re.search(r'\((\d{1,2}/\d{1,2}/\d{4}), \d{1,2}:\d{2}\)', title_text)
            
            if date_match:
                fecha_str = date_match.group(1)
                evento_nombre = title_text.split(' (')[0].strip()
                
                # Extract artist (first word or first part before dash)
                if ' - ' in evento_nombre:
                    artista = evento_nombre.split(' - ')[0].strip()
                    venue = evento_nombre.split(' - ')[1].strip()
                else:
                    artista = evento_nombre.split()[0] if evento_nombre else "Unknown"
                    venue = "Unknown"
                
                # Parse date
                try:
                    fecha_evento = datetime.strptime(fecha_str, '%d/%m/%Y').date()
                except:
                    fecha_evento = datetime.now().date()
                
                self.logger.info(f"🔍 DEBUG: evento_nombre='{evento_nombre}', artista='{artista}', venue='{venue}'")
                
                return {
                    'evento_nombre': evento_nombre,
                    'artista': artista,
                    'venue': venue,
                    'fecha_evento': fecha_evento.strftime('%Y-%m-%d')
                }
            else:
                # Fallback for titles without date format
                return {
                    'evento_nombre': title_text,
                    'artista': "Unknown",
                    'venue': "Unknown",
                    'fecha_evento': datetime.now().date().strftime('%Y-%m-%d')
                }
                
        except Exception as e:
            self.logger.error(f"Error extrayendo info del evento: {e}")
            return {
                'evento_nombre': title_text,
                'artista': "Unknown",
                'venue': "Unknown",
                'fecha_evento': datetime.now().date().strftime('%Y-%m-%d')
            }
    
    def save_single_event_to_database(self, df, title_text, report_index):
        """Save single event data to database"""
        try:
            if not self.db_connection:
                self.logger.error("No hay conexión a la base de datos")
                return False
            
            # Extract event information
            event_info = self.extract_event_info(title_text)
            
            # Calculate totals
            totales = self.calculate_event_totals(df, title_text)
            
            # Create structured data
            json_individual = {
                'evento': event_info['evento_nombre'],
                'artista': event_info['artista'],
                'venue': event_info['venue'],
                'fecha_evento': event_info['fecha_evento'],
                'reporte_index': report_index,
                'titulo_reporte': title_text,
                'totales_evento': totales,
                'datos_tabla': {
                    'headers': df.columns.tolist(),
                    'data': df.values.tolist(),
                    'shape': {'rows': df.shape[0], 'columns': df.shape[1]}
                }
            }
            
            # Convert to Argentina timezone (UTC-3)
            fecha_extraccion_argentina = datetime.now(timezone.utc) - timedelta(hours=3)
            
            # Insert into database
            cursor = self.db_connection.cursor()
            insert_query = """
                INSERT INTO raw_data (
                    ticketera, artista, venue, fecha_show, json_data, 
                    fecha_extraccion, archivo_origen, url_origen, procesado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            
            cursor.execute(insert_query, (
                'protickets',
                event_info['artista'],
                event_info['venue'],
                event_info['fecha_evento'],
                json.dumps(json_individual),
                fecha_extraccion_argentina,
                f'protickets_report_{report_index}',
                'https://dashboard.protickets.com.ar/',
                False
            ))
            
            record_id = cursor.fetchone()[0]
            self.db_connection.commit()
            cursor.close()
            
            self.logger.info(f"✅ Datos de '{event_info['evento_nombre']}' guardados exitosamente en la BD (ID: {record_id})")
            print(f"💾 GUARDADO EN BD: {event_info['artista']} - {event_info['venue']} - ID: {record_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error guardando en BD: {e}")
            print(f"❌ Error guardando en BD: {e}")
            return False
    
    def save_data_to_database(self, all_dataframes):
        """Save all extracted data to database"""
        try:
            if not all_dataframes:
                self.logger.warning("No hay datos para guardar")
                return
            
            self.logger.info(f"💾 Datos ya guardados durante la extracción individual de reportes")
            self.logger.info(f"✅ Total de reportes procesados: {len(all_dataframes)}")
            
            # Los datos ya se guardaron durante la extracción individual
            # No necesitamos guardarlos nuevamente aquí
            
        except Exception as e:
            self.logger.error(f"Error en save_data_to_database: {e}")
    
    def process_all_reports(self, reports_links):
        """Process all reports and extract table data"""
        if not reports_links:
            self.logger.warning("No reports to process")
            return []
        
        all_dataframes = []
        
        for i, report_url in enumerate(reports_links, 1):
            try:
                self.logger.info(f"Processing report {i} of {len(reports_links)}")
                
                # Extract table data from this report
                df = self.extract_table_from_report(report_url, i)
                
                if df is not None:
                    all_dataframes.append({
                        'report_index': i,
                        'report_url': report_url,
                        'dataframe': df
                    })
                
                # Wait a bit between reports to avoid overwhelming the server
                time.sleep(3)
                
            except Exception as e:
                self.logger.error(f"Error processing report {i}: {e}")
                continue
        
        self.logger.info(f"Successfully processed {len(all_dataframes)} out of {len(reports_links)} reports")
        return all_dataframes
    
    def run(self):
        """Main execution method"""
        try:
            self.logger.info("=== INICIANDO SCRAPER DE PROTICKETS ===")
            self.setup_driver()
            
            # Login
            if not self.login():
                self.logger.error("Login failed")
                return False
            
            # Wait for events icon and click it
            if not self.wait_for_events_icon():
                self.logger.error("Failed to navigate to events section")
                return False
            
            # Extract Reports links from item-box divs
            reports_links = self.extract_reports_links()
            
            # Process all reports and extract table data
            if reports_links:
                all_dataframes = self.process_all_reports(reports_links)
                
                # Save to database
                if all_dataframes:
                    self.save_data_to_database(all_dataframes)
                    
                    # Summary of all extracted data
                    print(f"\n=== SUMMARY ===")
                    print(f"Successfully extracted data from {len(all_dataframes)} reports")
                    for report_data in all_dataframes:
                        print(f"Report {report_data['report_index']}: {report_data['dataframe'].shape[0]} rows x {report_data['dataframe'].shape[1]} columns")
                else:
                    print("No table data could be extracted from any reports")
            
            self.logger.info("=== SCRAPER EJECUTADO EXITOSAMENTE ===")
            return True
            
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return False
        finally:
            if self.driver:
                self.driver.quit()
                self.logger.info("Driver closed")

def run_scraper_for_airflow():
    """Función para ejecutar el scraper desde Airflow"""
    try:
        scraper = ProticketsScraper(headless=True)
        success = scraper.run()
        
        if success:
            print("✅ Scraper ejecutado exitosamente")
            return True
        else:
            print("❌ Scraper falló")
            return False
            
    except Exception as e:
        print(f"❌ Error ejecutando scraper: {e}")
        return False

if __name__ == "__main__":
    scraper = ProticketsScraper(headless=True)
    success = scraper.run()
    
    if success:
        print("✅ Scraper ejecutado exitosamente")
    else:
        print("❌ Scraper falló. Revisa los logs para más detalles.")
