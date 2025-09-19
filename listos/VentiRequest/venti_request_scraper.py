#!/usr/bin/env python3
"""
Venti Request Scraper
Scraper para obtener datos de eventos de Venti usando su API REST
"""

import requests
import json
import os
from datetime import datetime, timedelta
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from database_config import get_database_connection

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('venti_request_scraper.log'),
        logging.StreamHandler()
    ]
)

class VentiRequestScraper:
    def __init__(self, save_json=True, save_to_db=False, simulate_db=False):
        self.base_url = "https://venti.com.ar/api"
        self.session = requests.Session()
        self.token = None
        self.user_id = None
        self.user_events = []  # Lista de eventos del usuario
        self.producer_ids = []  # IDs de productores del usuario
        self.pinned_events = []  # Eventos destacados
        self.save_json = save_json
        self.save_to_db = save_to_db
        self.simulate_db = simulate_db
        
        # Inicializar conexión a base de datos
        self.db_connection = None
        self.db_connected = False
        self.setup_database_connection()
        
        # Headers básicos
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })
        
        # Crear directorio para JSONs si no existe (solo si se guardará)
        if self.save_json:
            os.makedirs('jsonventi', exist_ok=True)

    def authenticate(self, email, password):
        """
        Autenticar con la API de Venti
        """
        try:
            auth_url = f"{self.base_url}/authenticate"
            
            payload = {
                "mail": email,
                "password": password,
                "platform": "web"
            }
            
            logging.info(f"Intentando autenticar con email: {email}")
            
            response = self.session.post(auth_url, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                logging.info("Autenticación exitosa")
                
                # Extraer token y user_id de la respuesta
                self.token = data.get('token')
                self.user_id = data.get('id') or data.get('user_id') or data.get('userId')
                
                # Extraer información útil de la respuesta de autenticación
                if 'events' in data:
                    self.user_events = data['events']
                    logging.info(f"Encontrados {len(self.user_events)} eventos en la respuesta de autenticación")
                
                # Extraer información de producers y eventos destacados
                user_info = data.get('user', {})
                producers = user_info.get('producers', [])
                
                if producers:
                    self.producer_ids = [p.get('id') for p in producers if p.get('id')]
                    logging.info(f"Encontrados {len(self.producer_ids)} producers: {self.producer_ids}")
                    
                    # Buscar eventos destacados
                    for producer in producers:
                        pinned_event = producer.get('pinnedEventId')
                        if pinned_event:
                            logging.info(f"Evento destacado encontrado: {pinned_event}")
                            if not hasattr(self, 'pinned_events'):
                                self.pinned_events = []
                            self.pinned_events.append(pinned_event)
                else:
                    self.producer_ids = []
                
                # Actualizar headers con el token
                if self.token:
                    self.session.headers.update({
                        'Authorization': f'Bearer {self.token}'
                    })
                
                # Guardar datos de autenticación
                if self.save_json:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    auth_filename = f"jsonventi/venti_auth_{timestamp}.json"
                    with open(auth_filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    logging.info(f"Datos de autenticación guardados en: {auth_filename}")
                
                return data
                
            else:
                logging.error(f"Error en autenticación: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logging.error(f"Error durante la autenticación: {str(e)}")
            return None

    def get_user_events(self):
        """
        Obtener la lista de eventos del usuario autenticado
        Intenta varios endpoints posibles para encontrar eventos
        """
        if not self.token:
            logging.error("No hay token de autenticación. Debe autenticarse primero.")
            return []
        
        # Lista de endpoints posibles para obtener eventos
        possible_endpoints = [
            "/events",
            "/user/events", 
            "/dashboard",
            "/report/events",
            "/my-events",
            "/user/dashboard",
            f"/user/{self.user_id}/events" if self.user_id else None
        ]
        
        # Agregar endpoints específicos de producers si tenemos IDs
        if hasattr(self, 'producer_ids') and self.producer_ids:
            for producer_id in self.producer_ids:
                possible_endpoints.extend([
                    f"/producer/{producer_id}/events",
                    f"/producer/{producer_id}/dashboard",
                    f"/producers/{producer_id}/events",
                    f"/report/producer/{producer_id}/events"
                ])
        
        # Si tenemos eventos destacados, probar obtenerlos directamente
        pinned_events = []
        if hasattr(self, 'pinned_events') and self.pinned_events:
            for pinned_event in self.pinned_events:
                logging.info(f"Probando obtener evento destacado directamente: {pinned_event}")
                try:
                    event_data = self.get_event_data(pinned_event)
                    if event_data:
                        # Crear un objeto evento simplificado
                        pinned_events.append({
                            'id': pinned_event,
                            'name': f'Evento Destacado {pinned_event}',
                            'source': 'pinned'
                        })
                        # Evitar caracteres no soportados por algunas consolas Windows
                        logging.info(f"Evento destacado {pinned_event} obtenido exitosamente")
                except Exception as e:
                    logging.error(f"Error obteniendo evento destacado {pinned_event}: {e}")
        
        # Filtrar endpoints None
        possible_endpoints = [ep for ep in possible_endpoints if ep is not None]
        
        events_found = []
        
        for endpoint in possible_endpoints:
            try:
                url = f"{self.base_url}{endpoint}"
                logging.info(f"Probando endpoint: {endpoint}")
                
                response = self.session.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Buscar eventos en diferentes estructuras posibles
                    events = self._extract_events_from_response(data)
                    
                    if events:
                        logging.info(f"✅ Encontrados {len(events)} eventos en {endpoint}")
                        events_found.extend(events)
                        
                        # Guardar respuesta para debug
                        if self.save_json:
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            debug_filename = f"jsonventi/venti_events_response_{endpoint.replace('/', '_')}_{timestamp}.json"
                            with open(debug_filename, 'w', encoding='utf-8') as f:
                                json.dump(data, f, ensure_ascii=False, indent=2)
                            logging.info(f"Respuesta guardada en: {debug_filename}")
                    else:
                        logging.info(f"No se encontraron eventos en {endpoint}")
                        
                elif response.status_code == 404:
                    logging.info(f"Endpoint {endpoint} no existe (404)")
                else:
                    logging.warning(f"Error en {endpoint}: {response.status_code} - {response.text[:200]}")
                    
            except Exception as e:
                logging.error(f"Error probando endpoint {endpoint}: {str(e)}")
                continue
        
        # Remover duplicados basándose en ID
        unique_events = []
        seen_ids = set()
        
        for event in events_found:
            event_id = event.get('id') or event.get('event_id') or event.get('eventId')
            if event_id and event_id not in seen_ids:
                unique_events.append(event)
                seen_ids.add(event_id)
        
        # Agregar eventos destacados a los encontrados
        if pinned_events:
            unique_events.extend(pinned_events)
            logging.info(f"Se agregaron {len(pinned_events)} eventos destacados")
        
        self.user_events = unique_events
        logging.info(f"Total de eventos únicos encontrados: {len(unique_events)}")
        
        return unique_events
    
    def _extract_events_from_response(self, data):
        """
        Extrae eventos de diferentes estructuras de respuesta posibles
        """
        events = []
        
        # Caso 1: data es directamente una lista de eventos
        if isinstance(data, list):
            events.extend(data)
        
        # Caso 2: data es un objeto con una clave 'events'
        elif isinstance(data, dict):
            # Buscar en diferentes claves posibles
            event_keys = ['events', 'data', 'items', 'results', 'content']
            
            for key in event_keys:
                if key in data and isinstance(data[key], list):
                    events.extend(data[key])
                    break
            
            # Si no encontramos en claves obvias, buscar objetos que parezcan eventos
            if not events:
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
                        # Verificar si los elementos parecen eventos
                        first_item = value[0]
                        if isinstance(first_item, dict) and self._looks_like_event(first_item):
                            events.extend(value)
                            break
        
        # Filtrar y validar eventos
        valid_events = []
        for event in events:
            if isinstance(event, dict) and self._looks_like_event(event):
                valid_events.append(event)
        
        return valid_events
    
    def _looks_like_event(self, item):
        """
        Determina si un objeto parece ser un evento
        """
        if not isinstance(item, dict):
            return False
        
        # Buscar campos típicos de eventos
        event_indicators = [
            'id', 'event_id', 'eventId',
            'name', 'title', 'event_name',
            'date', 'start_date', 'event_date',
            'venue', 'location', 'place'
        ]
        
        # Si tiene al menos 2 indicadores, probablemente es un evento
        indicators_found = sum(1 for indicator in event_indicators if indicator in item)
        return indicators_found >= 2

    def get_event_data(self, event_id):
        """
        Obtener datos de un evento específico desde múltiples endpoints
        """
        try:
            if not self.token:
                logging.error("No hay token de autenticación. Debe autenticarse primero.")
                return None
            
            # Lista de endpoints posibles para obtener información del evento
            possible_endpoints = [
                f"/report/event/{event_id}",
                f"/event/{event_id}",
                f"/events/{event_id}",
                f"/event/{event_id}/details",
                f"/event/{event_id}/info",
                f"/event/{event_id}/data",
                f"/api/event/{event_id}",
                f"/api/events/{event_id}"
            ]
            
            all_data = {}
            
            for endpoint in possible_endpoints:
                try:
                    url = f"{self.base_url}{endpoint}"
                    logging.info(f"Probando endpoint: {endpoint}")
                    
                    response = self.session.get(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        logging.info(f"✅ Datos obtenidos de {endpoint}")
                        
                        # Combinar datos de diferentes endpoints
                        all_data.update(data)
                        
                        # Guardar respuesta individual para debug
                        if self.save_json:
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            debug_filename = f"jsonventi/venti_event_{event_id}_{endpoint.replace('/', '_')}_{timestamp}.json"
                            with open(debug_filename, 'w', encoding='utf-8') as f:
                                json.dump(data, f, ensure_ascii=False, indent=2)
                            logging.info(f"Respuesta guardada en: {debug_filename}")
                            
                    elif response.status_code == 404:
                        logging.info(f"Endpoint {endpoint} no existe (404)")
                    else:
                        logging.warning(f"Error en {endpoint}: {response.status_code} - {response.text[:200]}")
                        
                except Exception as e:
                    logging.error(f"Error probando endpoint {endpoint}: {str(e)}")
                    continue
            
            if all_data:
                logging.info(f"Datos del evento {event_id} obtenidos exitosamente")
                
                # Guardar datos consolidados del evento
                if self.save_json:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    event_filename = f"jsonventi/venti_event_{event_id}_consolidated_{timestamp}.json"
                    with open(event_filename, 'w', encoding='utf-8') as f:
                        json.dump(all_data, f, ensure_ascii=False, indent=2)
                    logging.info(f"Datos consolidados guardados en: {event_filename}")
                
                return all_data
            else:
                logging.error(f"No se pudieron obtener datos del evento {event_id} desde ningún endpoint")
                return None
                
        except Exception as e:
            logging.error(f"Error obteniendo datos del evento {event_id}: {str(e)}")
            return None

    def get_all_events_data(self, event_ids):
        """
        Obtener datos de múltiples eventos
        """
        all_events_data = []
        
        for event_id in event_ids:
            event_data = self.get_event_data(event_id)
            if event_data:
                all_events_data.append({
                    'event_id': event_id,
                    'data': event_data
                })
        
        if all_events_data and self.save_json:
            # Guardar todos los eventos en un archivo consolidado
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            all_events_filename = f"jsonventi/venti_all_events_{timestamp}.json"
            with open(all_events_filename, 'w', encoding='utf-8') as f:
                json.dump(all_events_data, f, ensure_ascii=False, indent=2)
            logging.info(f"Datos de todos los eventos guardados en: {all_events_filename}")
        
        return all_events_data

    def run_scraper(self, email, password, event_ids=None, preview=False):
        """
        Ejecutar el scraper completo
        Si event_ids es None, obtiene automáticamente la lista de eventos
        """
        logging.info("=== Iniciando Venti Request Scraper ===")
        
        # Paso 1: Autenticar
        auth_data = self.authenticate(email, password)
        if not auth_data:
            logging.error("Falló la autenticación. Terminando scraper.")
            return False
        
        # Paso 2: Obtener lista de eventos si no se proporcionaron
        if event_ids is None or (isinstance(event_ids, list) and len(event_ids) == 0):
            logging.info("Obteniendo lista de eventos automáticamente...")
            discovered_events = self.get_user_events()
            
            if discovered_events:
                # Extraer IDs de los eventos encontrados
                event_ids = []
                for event in discovered_events:
                    event_id = event.get('id') or event.get('event_id') or event.get('eventId')
                    if event_id:
                        event_ids.append(event_id)
                
                logging.info(f"Se encontraron {len(event_ids)} eventos: {event_ids}")
            else:
                logging.warning("No se encontraron eventos automáticamente")
                return False
        
        # Paso 3: Obtener datos de eventos
        if isinstance(event_ids, list) and len(event_ids) > 0:
            # Múltiples eventos
            all_events_data = self.get_all_events_data(event_ids)
            if preview:
                for item in all_events_data:
                    self.print_preview(item.get('data'), item.get('event_id'))
            success = len(all_events_data) > 0
        elif event_ids is not None:
            # Un solo evento
            event_data = self.get_event_data(event_ids)
            if preview and event_data:
                self.print_preview(event_data, event_ids)
            success = event_data is not None
        else:
            logging.error("No se proporcionaron IDs de eventos")
            success = False
        
        # Paso 4: Guardar en base de datos si se solicita
        if success and (self.save_to_db or self.simulate_db):
            if isinstance(event_ids, list) and len(event_ids) > 0:
                # Múltiples eventos
                for item in all_events_data:
                    extracted = self.extract_show_and_daily_sales(item.get('data'), item.get('event_id'))
                    if extracted:
                        if self.simulate_db:
                            self.simulate_database_save(extracted)
                        else:
                            self.save_to_database(extracted)
            elif event_ids is not None:
                # Un solo evento
                extracted = self.extract_show_and_daily_sales(event_data, event_ids)
                if extracted:
                    if self.simulate_db:
                        self.simulate_database_save(extracted)
                    else:
                        self.save_to_database(extracted)
        
        if success:
            logging.info("=== Scraper completado exitosamente ===")
        else:
            logging.error("=== Scraper terminó con errores ===")
            
        return success

    def extract_show_and_daily_sales(self, event_data, event_id=None):
        """
        Extrae datos del show y ventas diarias en formato limpio
        """
        try:
            report = (event_data or {}).get('report', {})
            
            # Extraer datos básicos del show
            show_data = {
                'event_id': event_id,
                'ticketera': 'VentiRequest',
                'fecha_extraccion': datetime.now().isoformat()
            }
            
            # Buscar información del evento en diferentes lugares posibles
            totals = report.get('eventTotalSales', {})
            by_type = report.get('eventSalesAndUsedGroupByTicketType', {})
            event_info = report.get('eventInfo', {})
            
            # Extraer información del evento principal (desde el endpoint /event/{id})
            event_main = event_data.get('event', {})
            venue_info = event_main.get('venue', {})
            city_info = venue_info.get('city', {})
            
            # Extraer nombre del evento
            event_name = event_main.get('name') or event_info.get('name', f'Evento {event_id}')
            
            # Extraer artista del nombre del evento
            artista = event_name
            if ' - ' in event_name:
                artista = event_name.split(' - ')[0].strip()
            elif ' Festival' in event_name:
                # Para festivales, usar el nombre completo como artista
                artista = event_name
            
            # Extraer fecha del evento
            fecha_show = None
            start_date = event_main.get('startDate')
            if start_date:
                try:
                    # Convertir de ISO format a YYYY-MM-DD
                    fecha_show = datetime.fromisoformat(start_date.replace('Z', '+00:00')).strftime('%Y-%m-%d')
                except:
                    pass
            
            # Extraer información del venue
            venue_name = venue_info.get('placeName', 'Venti')
            venue_address = venue_info.get('readableAddress', '')
            city_name = city_info.get('name', '')
            country = city_info.get('country', '')
            
            show_data.update({
                'artista': artista,
                'evento_nombre': event_name,
                'venue': venue_name,
                'venue_address': venue_address,
                'ciudad': city_name,
                'pais': country,
                'fecha_show': fecha_show,
                'capacidad_total': by_type.get('totalStock', 0),
                'vendido_total': by_type.get('totalSold', 0),
                'recaudacion_total_ars': totals.get('totalSales', 0),
                'disponible_total': by_type.get('totalStock', 0) - by_type.get('totalSold', 0)
            })
            
            # Calcular porcentaje de ocupación
            if show_data['capacidad_total'] > 0:
                show_data['porcentaje_ocupacion'] = round(
                    (show_data['vendido_total'] / show_data['capacidad_total']) * 100, 2
                )
            else:
                show_data['porcentaje_ocupacion'] = 0
            
            # Extraer ventas diarias
            daily_sales = []
            per_day = report.get('eventSalesPerDay', [])
            
            if per_day and isinstance(per_day, list):
                for item in per_day:
                    try:
                        # Parsear fecha (formato DD/MM/YYYY)
                        fecha_str = item.get('date', '')
                        if fecha_str:
                            # Convertir DD/MM/YYYY a YYYY-MM-DD
                            parts = fecha_str.split('/')
                            if len(parts) == 3:
                                fecha_iso = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
                                
                                daily_sales.append({
                                    'fecha_venta': fecha_iso,
                                    'tickets_vendidos': item.get('ticketsNumber', 0),
                                    'monto_diario_ars': item.get('totalIncome', 0),
                                    'event_id': event_id
                                })
                    except Exception as e:
                        logging.warning(f"Error parseando fecha {item.get('date')}: {e}")
                        continue
            
            return {
                'show_data': show_data,
                'daily_sales': daily_sales
            }
            
        except Exception as e:
            logging.error(f"Error extrayendo datos del show: {e}")
            return None

    def print_preview(self, event_data, event_id=None):
        """
        Imprime un resumen compacto de los datos del evento (sin guardar JSON)
        """
        try:
            extracted = self.extract_show_and_daily_sales(event_data, event_id)
            if not extracted:
                print("Error extrayendo datos del evento")
                return
                
            show_data = extracted['show_data']
            daily_sales = extracted['daily_sales']
            
            print("\n=== PREVIEW EVENTO ===")
            print(f"event_id: {show_data['event_id']}")
            print(f"artista: {show_data['artista']}")
            print(f"evento_nombre: {show_data['evento_nombre']}")
            print(f"fecha_show: {show_data['fecha_show']}")
            print(f"venue: {show_data['venue']}")
            print(f"venue_address: {show_data['venue_address']}")
            print(f"ciudad: {show_data['ciudad']}")
            print(f"pais: {show_data['pais']}")
            print(f"capacidad_total: {show_data['capacidad_total']}")
            print(f"vendido_total: {show_data['vendido_total']}")
            print(f"disponible_total: {show_data['disponible_total']}")
            print(f"recaudacion_total_ars: {show_data['recaudacion_total_ars']}")
            print(f"porcentaje_ocupacion: {show_data['porcentaje_ocupacion']}%")
            
            if daily_sales:
                print(f"ventas diarias ({len(daily_sales)} fechas):")
                for item in daily_sales[:10]:  # Mostrar primeras 10
                    print(f"  {item['fecha_venta']}: tickets={item['tickets_vendidos']} monto={item['monto_diario_ars']}")
                if len(daily_sales) > 10:
                    print(f"  ... y {len(daily_sales) - 10} fechas más")
            else:
                print("No hay ventas diarias disponibles")
                
            print("=== FIN PREVIEW ===\n")
        except Exception as e:
            logging.error(f"Error en print_preview: {e}")

    def setup_database_connection(self):
        """Verifica conexión con la base de datos PostgreSQL"""
        try:
            logging.info("🔌 Verificando conexión con la base de datos...")
            connection = get_database_connection()
            if connection:
                cursor = connection.cursor()
                cursor.execute("SELECT NOW();")
                result = cursor.fetchone()
                logging.info(f"✅ Conexión exitosa! Hora actual: {result[0]}")
                cursor.close()
                connection.close()
                self.db_connected = True
                return True
            else:
                logging.warning("⚠️ No se pudo establecer conexión a la base de datos")
                self.db_connected = False
                return False
        except Exception as e:
            logging.error(f"❌ Error en la conexión a la base de datos: {e}")
            self.db_connected = False
            return False

    def save_to_database(self, extracted_data):
        """Guarda los datos extraídos en la base de datos"""
        try:
            if not self.db_connected:
                logging.warning("⚠️ Base de datos no conectada, no se pueden guardar datos")
                return False
            
            if not extracted_data:
                logging.warning("No hay datos para guardar")
                return False
            
            show_data = extracted_data['show_data']
            daily_sales = extracted_data['daily_sales']
            
            logging.info(f"💾 Guardando datos en la base de datos...")
            
            # Obtener conexión
            connection = get_database_connection()
            if not connection:
                logging.error("❌ No se pudo obtener conexión a la base de datos")
                return False
            
            cursor = connection.cursor()
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            
            # 1. CREAR/OBTENER SHOW (solo crear si no existe)
            show_id = self.create_or_get_show_venti(cursor, show_data)
            
            # 2. PROCESAR DAILY_SALES (agregar/actualizar todas las fechas)
            if daily_sales and show_id:
                self.process_daily_sales_venti(cursor, show_id, show_data, daily_sales, fecha_extraccion_utc3)
            
            # Commit y cerrar conexión
            connection.commit()
            cursor.close()
            connection.close()
            
            logging.info(f"✅ Datos guardados exitosamente en la base de datos")
            return True
            
        except Exception as e:
            logging.error(f"❌ Error guardando datos en base de datos: {str(e)}")
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'connection' in locals() and connection:
                    connection.rollback()
                    connection.close()
            except:
                pass
            return False

    def save_raw_data_venti(self, cursor, show_data, extracted_data, fecha_extraccion):
        """Guarda datos en raw_data"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archivo_origen = f"venti_request_scraper_{timestamp}"
            
            # Crear JSON individual para este evento
            json_individual = {
                'evento': show_data.get('evento_nombre', ''),
                'artista': show_data.get('artista', ''),
                'venue': show_data.get('venue', ''),
                'fecha_evento': show_data.get('fecha_show', ''),
                'ciudad': show_data.get('ciudad', ''),
                'pais': show_data.get('pais', ''),
                'totales_evento': {
                    'capacidad_total': show_data.get('capacidad_total', 0),
                    'vendido_total': show_data.get('vendido_total', 0),
                    'disponible_total': show_data.get('disponible_total', 0),
                    'recaudacion_total_ars': show_data.get('recaudacion_total_ars', 0),
                    'porcentaje_ocupacion': show_data.get('porcentaje_ocupacion', 0)
                },
                'daily_sales': extracted_data.get('daily_sales', []),
                'fecha_extraccion': show_data.get('fecha_extraccion', '')
            }
            
            cursor.execute("""
                INSERT INTO raw_data (ticketera, artista, venue, fecha_show, archivo_origen, fecha_extraccion, json_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                "VentiRequest",
                show_data.get('artista', ''),
                show_data.get('venue', ''),
                show_data.get('fecha_show', ''),
                archivo_origen,
                fecha_extraccion.isoformat(),
                json.dumps(json_individual, ensure_ascii=False)
            ))
            
            result = cursor.fetchone()
            if result:
                logging.info(f"✅ Raw data guardado (ID: {result[0]})")
                return result[0]
            else:
                logging.warning("⚠️ Raw data guardado pero sin ID retornado")
                return None
                
        except Exception as e:
            logging.error(f"❌ Error guardando raw data: {str(e)}")
            return None

    def create_or_get_show_venti(self, cursor, show_data):
        """Busca show existente por artista y fecha, si no existe lo crea (sin actualizar existentes)"""
        try:
            artista = show_data.get('artista', '')
            fecha_show = show_data.get('fecha_show', '')
            
            if not artista or not fecha_show:
                logging.warning("⚠️ Show sin artista o fecha, saltando...")
                return None
            
            # Buscar show existente por artista, fecha y ticketera
            cursor.execute("""
                SELECT id FROM shows 
                WHERE artista = %s 
                AND DATE(fecha_show) = %s 
                AND ticketera = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (artista, fecha_show, "VentiRequest"))
            
            existing_show = cursor.fetchone()
            
            if existing_show:
                show_id = existing_show[0]
                logging.info(f"📋 Show existente encontrado (ID: {show_id}) - NO se actualiza")
                return show_id
            else:
                logging.info(f"🆕 Creando nuevo show para {artista} - {fecha_show}")
                
                # Crear nuevo show
                cursor.execute("""
                    INSERT INTO shows (artista, venue, fecha_show, ticketera, estado, capacidad_total, ciudad, pais)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    artista,
                    show_data.get('venue', ''),
                    fecha_show,
                    "VentiRequest",
                    "activo",
                    show_data.get('capacidad_total', 0),
                    show_data.get('ciudad', ''),
                    show_data.get('pais', 'Argentina')
                ))
                
                result = cursor.fetchone()
                if result:
                    show_id = result[0]
                    logging.info(f"✅ Nuevo show creado (ID: {show_id})")
                    return show_id
                else:
                    logging.error("❌ Error creando show")
                    return None
                    
        except Exception as e:
            logging.error(f"❌ Error en create_or_get_show: {str(e)}")
            return None

    def process_daily_sales_venti(self, cursor, show_id, show_data, daily_sales, fecha_extraccion):
        """Procesa daily_sales con datos de ventas diarias reales y acumulación progresiva"""
        try:
            if not show_id:
                logging.warning("⚠️ No hay show_id para procesar daily_sales")
                return
            
            if not daily_sales:
                logging.info("📅 No hay ventas diarias para procesar")
                return
            
            logging.info(f"📅 Procesando {len(daily_sales)} fechas de ventas diarias")
            
            # Ordenar fechas para calcular acumulados correctamente
            daily_sales_sorted = sorted(daily_sales, key=lambda x: x.get('fecha_venta', ''))
            
            venta_total_acumulada = 0
            recaudacion_total_acumulada = 0
            capacidad_total = show_data.get('capacidad_total', 0)
            
            for daily_data in daily_sales_sorted:
                fecha_venta = daily_data.get('fecha_venta')
                venta_diaria = daily_data.get('tickets_vendidos', 0)
                monto_diario = daily_data.get('monto_diario_ars', 0)
                
                if not fecha_venta:
                    continue
                
                # Calcular acumulados progresivos
                venta_total_acumulada += venta_diaria
                recaudacion_total_acumulada += monto_diario
                
                # Calcular disponibles y ocupación
                tickets_disponibles = capacidad_total - venta_total_acumulada
                porcentaje_ocupacion = round((venta_total_acumulada / capacidad_total) * 100, 2) if capacidad_total > 0 else 0
                
                # Campos USD y precios promedio se calculan después en el proceso de análisis
                precio_promedio_ars = 0
                precio_promedio_usd = 0
                monto_diario_usd = 0
                recaudacion_total_usd = 0
                
                # Buscar si ya existe un registro para esta fecha
                cursor.execute("""
                    SELECT id FROM daily_sales 
                    WHERE show_id = %s AND fecha_venta = %s
                """, (show_id, fecha_venta))
                
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # Actualizar registro existente
                    cursor.execute("""
                        UPDATE daily_sales SET
                            fecha_extraccion = %s,
                            venta_diaria = %s,
                            monto_diario_ars = %s,
                            monto_diario_usd = %s,
                            venta_total_acumulada = %s,
                            recaudacion_total_ars = %s,
                            recaudacion_total_usd = %s,
                            tickets_disponibles = %s,
                            porcentaje_ocupacion = %s,
                            precio_promedio_ars = %s,
                            precio_promedio_usd = %s,
                            updated_at = NOW()
                        WHERE show_id = %s AND fecha_venta = %s
                    """, (
                        fecha_extraccion.isoformat(), venta_diaria, monto_diario, monto_diario_usd,
                        venta_total_acumulada, recaudacion_total_acumulada, recaudacion_total_usd,
                        tickets_disponibles, porcentaje_ocupacion, precio_promedio_ars, precio_promedio_usd,
                        show_id, fecha_venta
                    ))
                    logging.info(f"📊 Daily sales actualizado para {fecha_venta} - Acumulado: {venta_total_acumulada}")
                else:
                    # Crear nuevo registro
                    cursor.execute("""
                        INSERT INTO daily_sales (
                            show_id, fecha_venta, fecha_extraccion, venta_diaria, monto_diario_ars, monto_diario_usd,
                            venta_total_acumulada, recaudacion_total_ars, recaudacion_total_usd, tickets_disponibles,
                            porcentaje_ocupacion, precio_promedio_ars, precio_promedio_usd, ticketera, archivo_origen
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        show_id, fecha_venta, fecha_extraccion.isoformat(), venta_diaria, monto_diario, monto_diario_usd,
                        venta_total_acumulada, recaudacion_total_acumulada, recaudacion_total_usd, tickets_disponibles,
                        porcentaje_ocupacion, precio_promedio_ars, precio_promedio_usd, "VentiRequest", f"venti_request_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    ))
                    logging.info(f"✅ Daily sales creado para {fecha_venta} - Acumulado: {venta_total_acumulada}")
                    
        except Exception as e:
            logging.error(f"❌ Error procesando daily_sales: {str(e)}")

    def simulate_database_save(self, extracted_data):
        """Simula el guardado en base de datos mostrando exactamente qué se ejecutaría"""
        try:
            if not extracted_data:
                logging.warning("No hay datos para simular")
                return False
            
            show_data = extracted_data['show_data']
            daily_sales = extracted_data['daily_sales']
            
            print("\n" + "="*80)
            print("🎭 SIMULACIÓN DE GUARDADO EN BASE DE DATOS")
            print("="*80)
            
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archivo_origen = f"venti_request_scraper_{timestamp}"
            
            # 1. SIMULAR SHOWS (buscar existente o crear nuevo)
            print("\n🎭 1. TABLA: shows")
            print("-" * 50)
            
            artista = show_data.get('artista', '')
            fecha_show = show_data.get('fecha_show', '')
            
            print("-- Buscar show existente:")
            print("SELECT id FROM shows")
            print("WHERE artista = '{}'".format(artista))
            print("AND DATE(fecha_show) = '{}'".format(fecha_show))
            print("AND ticketera = 'VentiRequest'")
            print("ORDER BY created_at DESC LIMIT 1;")
            
            print("\n-- Si NO existe, crear nuevo show:")
            print("INSERT INTO shows (artista, venue, fecha_show, ticketera, estado, capacidad_total, ciudad, pais)")
            print("VALUES (")
            print(f"  '{artista}',")
            print(f"  '{show_data.get('venue', '')}',")
            print(f"  '{fecha_show}',")
            print(f"  'VentiRequest',")
            print(f"  'activo',")
            print(f"  {show_data.get('capacidad_total', 0)},")
            print(f"  '{show_data.get('ciudad', '')}',")
            print(f"  '{show_data.get('pais', 'Argentina')}'")
            print(") RETURNING id;")
            
            print("\n-- Si SÍ existe, NO se actualiza nada (solo usar el ID)")
            print("-- show_id = <ID_obtenido_del_SELECT>")
            
            # 2. SIMULAR DAILY_SALES
            print("\n📊 2. TABLA: daily_sales")
            print("-" * 50)
            
            if daily_sales:
                print(f"-- Procesando {len(daily_sales)} fechas de ventas diarias")
                print("-- NOTA: Los totales se calculan PROGRESIVAMENTE (acumulando día a día)")
                
                # Ordenar fechas para mostrar cálculo progresivo
                daily_sales_sorted = sorted(daily_sales, key=lambda x: x.get('fecha_venta', ''))
                venta_total_acumulada = 0
                recaudacion_total_acumulada = 0
                capacidad_total = show_data.get('capacidad_total', 0)
                
                for i, daily_data in enumerate(daily_sales_sorted[:5]):  # Mostrar solo las primeras 5
                    fecha_venta = daily_data.get('fecha_venta')
                    venta_diaria = daily_data.get('tickets_vendidos', 0)
                    monto_diario = daily_data.get('monto_diario_ars', 0)
                    
                    # Calcular acumulados progresivos
                    venta_total_acumulada += venta_diaria
                    recaudacion_total_acumulada += monto_diario
                    tickets_disponibles = capacidad_total - venta_total_acumulada
                    porcentaje_ocupacion = round((venta_total_acumulada / capacidad_total) * 100, 2) if capacidad_total > 0 else 0
                    
                    # Campos USD y precios promedio se calculan después
                    precio_promedio_ars = 0
                    precio_promedio_usd = 0
                    monto_diario_usd = 0
                    recaudacion_total_usd = 0
                    
                    print(f"\n-- Fecha {i+1}: {fecha_venta}")
                    print(f"-- Ventas del día: {venta_diaria} | Monto del día: {monto_diario} ARS")
                    print(f"-- ACUMULADO: {venta_total_acumulada} tickets | {recaudacion_total_acumulada} ARS")
                    print(f"-- Disponibles: {tickets_disponibles} | Ocupación: {porcentaje_ocupacion}%")
                    print(f"-- USD y precios promedio: se calculan después")
                    
                    print("\n-- Buscar registro existente:")
                    print("SELECT id FROM daily_sales")
                    print("WHERE show_id = '<show_id_obtenido>' AND fecha_venta = '{}';".format(fecha_venta))
                    
                    print("\n-- Si NO existe, crear nuevo:")
                    print("INSERT INTO daily_sales (")
                    print("  show_id, fecha_venta, fecha_extraccion, venta_diaria, monto_diario_ars,")
                    print("  venta_total_acumulada, recaudacion_total_ars, tickets_disponibles,")
                    print("  porcentaje_ocupacion, ticketera, archivo_origen")
                    print(") VALUES (")
                    print("  '<show_id_obtenido>',")
                    print(f"  '{fecha_venta}',")
                    print(f"  '{fecha_extraccion_utc3.isoformat()}',")
                    print(f"  {venta_diaria},")
                    print(f"  {monto_diario},")
                    print(f"  {venta_total_acumulada},")
                    print(f"  {recaudacion_total_acumulada},")
                    print(f"  {tickets_disponibles},")
                    print(f"  {porcentaje_ocupacion},")
                    print("  'VentiRequest',")
                    print(f"  'venti_request_{timestamp}'")
                    print(");")
                    
                    print("\n-- Si SÍ existe, actualizar:")
                    print("UPDATE daily_sales SET")
                    print(f"  fecha_extraccion = '{fecha_extraccion_utc3.isoformat()}',")
                    print(f"  venta_diaria = {venta_diaria},")
                    print(f"  monto_diario_ars = {monto_diario},")
                    print(f"  venta_total_acumulada = {venta_total_acumulada},")
                    print(f"  recaudacion_total_ars = {recaudacion_total_acumulada},")
                    print(f"  tickets_disponibles = {tickets_disponibles},")
                    print(f"  porcentaje_ocupacion = {porcentaje_ocupacion},")
                    print("  updated_at = NOW()")
                    print("WHERE show_id = '<show_id_obtenido>' AND fecha_venta = '{}';".format(fecha_venta))
                
                if len(daily_sales) > 5:
                    print(f"\n... y {len(daily_sales) - 5} fechas más con el mismo patrón")
            else:
                print("-- No hay ventas diarias para procesar")
            
            # 3. RESUMEN DE CAMPOS MAPEADOS
            print("\n📋 3. RESUMEN DE CAMPOS MAPEADOS")
            print("-" * 50)
            print("SHOWS:")
            print(f"  ✅ artista: '{show_data.get('artista', '')}'")
            print(f"  ✅ venue: '{show_data.get('venue', '')}'")
            print(f"  ✅ fecha_show: '{show_data.get('fecha_show', '')}'")
            print(f"  ✅ ticketera: 'VentiRequest'")
            print(f"  ✅ estado: 'activo'")
            print(f"  ✅ capacidad_total: {show_data.get('capacidad_total', 0)}")
            print(f"  ✅ ciudad: '{show_data.get('ciudad', '')}'")
            print(f"  ✅ pais: '{show_data.get('pais', 'Argentina')}'")
            
            print("\nDAILY_SALES (por cada fecha):")
            print("  ✅ show_id: <obtenido_de_shows>")
            print("  ✅ fecha_venta: <fecha_específica>")
            print("  ✅ fecha_extraccion: <timestamp_actual>")
            print("  ✅ venta_diaria: <cantidad_ventas_ese_día>")
            print("  ✅ monto_diario_ars: <monto_ese_día>")
            print("  ✅ venta_total_acumulada: <total_vendido_evento>")
            print("  ✅ recaudacion_total_ars: <total_recaudado_evento>")
            print("  ✅ tickets_disponibles: <disponibles_evento>")
            print("  ✅ porcentaje_ocupacion: <%_ocupacion_evento>")
            print("  ✅ ticketera: 'VentiRequest'")
            print("  ✅ archivo_origen: <timestamp_scraper>")
            print("\n💡 NOTA: Se agregan/actualizan TODAS las fechas de ventas diarias")
            
            print("\n" + "="*80)
            print("🎭 FIN DE SIMULACIÓN - NINGÚN DATO FUE GUARDADO REALMENTE")
            print("="*80 + "\n")
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Error en simulación: {str(e)}")
            return False

def main():
    """
    Función principal para ejecutar el scraper
    """
    import argparse
    parser = argparse.ArgumentParser(description='Venti Request Scraper')
    parser.add_argument('--email', default='fatima.villegas@daleplay.la')
    parser.add_argument('--password', default='@Daleplay2025')
    parser.add_argument('--event-id', type=int, help='ID de evento único')
    parser.add_argument('--event-ids', nargs='*', type=int, help='IDs de eventos (múltiples)')
    parser.add_argument('--no-save', action='store_true', help='No guardar JSONs en disco')
    parser.add_argument('--preview', action='store_true', help='Imprimir resumen de datos extraídos')
    parser.add_argument('--save-db', action='store_true', help='Guardar datos en la base de datos')
    parser.add_argument('--simulate-db', action='store_true', help='Simular guardado en base de datos (sin ejecutar)')
    args = parser.parse_args()

    EVENT_IDS = args.event_ids if args.event_ids else ([] if args.event_id is None else args.event_id)
    scraper = VentiRequestScraper(save_json=not args.no_save, save_to_db=args.save_db, simulate_db=args.simulate_db)
    
    if not EVENT_IDS:
        logging.info("No se especificaron IDs de eventos. Obteniendo eventos automáticamente...")
        logging.info("Para especificar eventos manualmente: use --event-id o --event-ids")
    
    scraper.run_scraper(args.email, args.password, EVENT_IDS if EVENT_IDS else None, preview=args.preview)

if __name__ == "__main__":
    main()