from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
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
from artistas_conocidos import validar_artista, agregar_artista_nuevo

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TeleticketScraper:
    def __init__(self, headless=True):
        """
        Inicializa el scraper de Teleticket para Airflow
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless
        """
        self.driver = None
        self.headless = headless
        self.base_url = "https://backoffice.teleticket.com.pe/Account/LogOn"
        
        # Lista de credenciales para probar una por una
        self.credentials = [
            # {"username": "fvillegas", "password": "Fv0896"},  # Comentada temporalmente
            {"username": "magiglio", "password": "Booking5863"},  # Solo esta activa por ahora
            # {"username": "ERREWAY2025", "password": "ERREWAY25"}  # Comentada temporalmente
        ]
        self.current_credential_index = 0
        
        # Configuración para contenedores (Airflow)
        self.download_folder = "/tmp"
        
        # Estructura de datos para Airflow
        self.final_data = {
            "ticketera": "Teleticket",
            "fecha_extraccion": "",
            "total_eventos_procesados": 0,
            "eventos_exitosos": 0,
            "eventos_con_error": 0,
            "datos_por_evento": {}
        }
        
        # Conexión a base de datos
        self.db_connection = None
        self.setup_database_connection()
        
    def setup_database_connection(self):
        """Establece conexión con la base de datos"""
        try:
            self.db_connection = get_database_connection()
            if self.db_connection:
                logger.info("✅ Conexión a base de datos establecida")
                return True
            else:
                logger.error("❌ No se pudo establecer conexión a la base de datos")
                return False
        except Exception as e:
            logger.error(f"❌ Error estableciendo conexión a BD: {str(e)}")
            return False
    
    def extract_artist_name(self, evento_nombre):
        """
        Extrae solo el nombre del artista del nombre completo del evento usando la lista de artistas conocidos
        
        Args:
            evento_nombre (str): Nombre completo del evento (ej: "VEN057 - CAZZU - LATINAJE EN VIVO - MULTIESPACIO COSTA 21")
            
        Returns:
            str: Solo el nombre del artista (ej: "CAZZU")
        """
        try:
            if not evento_nombre:
                return ""
            
            # Limpiar saltos de línea y espacios extra
            evento_limpio = evento_nombre.replace('\n', ' ').strip()
            
            # PRIMERO: Buscar si algún artista conocido está en el evento
            from artistas_conocidos import ARTISTAS_CONOCIDOS
            
            for artista_conocido in ARTISTAS_CONOCIDOS:
                if artista_conocido.upper() in evento_limpio.upper():
                    logger.info(f"🎯 Artista encontrado en lista: '{artista_conocido}' en evento '{evento_limpio}'")
                    return artista_conocido
            
            # SEGUNDO: Si no se encuentra en la lista, usar la lógica de extracción original
            artista_extraido = ""
            
            # Si contiene código de evento (ej: "VEN057 - CAZZU - LATINAJE EN VIVO - MULTIESPACIO COSTA 21")
            if " - " in evento_limpio:
                parts = evento_limpio.split(" - ")
                if len(parts) >= 2:
                    # El artista está en la segunda parte después del código
                    # Formato: "VEN057 - CAZZU - LATINAJE EN VIVO - MULTIESPACIO COSTA 21"
                    # parts[0] = "VEN057" (código)
                    # parts[1] = "CAZZU" (artista) ← Esta es la parte que necesitamos
                    artista_extraido = parts[1].strip()
            
            # Dividir por " EN " para separar artista de ciudad (formato anterior)
            elif " EN " in evento_limpio:
                parts = evento_limpio.split(" EN ")
                # El artista está en la primera parte antes de " EN "
                # Ejemplo: "ERREWAY EN AREQUIPA" -> artista = "ERREWAY"
                artista_extraido = parts[0].strip()
            else:
                # Si no hay " EN ", tomar la primera palabra
                words = evento_limpio.split()
                if words:
                    artista_extraido = words[0].strip()
                else:
                    artista_extraido = evento_limpio.strip()
            
            # Validar el artista usando la lista de artistas conocidos
            artista_validado = validar_artista(artista_extraido)
            
            # Si el artista validado es diferente al extraído, loggear la corrección
            if artista_validado != artista_extraido:
                logger.info(f"🎯 Artista corregido: '{artista_extraido}' -> '{artista_validado}'")
            
            return artista_validado
                
        except Exception as e:
            logger.error(f"Error extrayendo nombre del artista: {str(e)}")
            return evento_nombre or ""
    
    def extract_venue_name(self, evento_nombre):
        """
        Extrae el nombre del venue del nombre completo del evento
        
        Args:
            evento_nombre (str): Nombre completo del evento (ej: "VEN057 - CAZZU - LATINAJE EN VIVO - MULTIESPACIO COSTA 21")
            
        Returns:
            str: Nombre del venue (ej: "MULTIESPACIO COSTA 21")
        """
        try:
            if not evento_nombre:
                return ""
            
            # Limpiar saltos de línea y espacios extra
            evento_limpio = evento_nombre.replace('\n', ' ').strip()
            
            # Si contiene código de evento (ej: "VEN057 - CAZZU - LATINAJE EN VIVO - MULTIESPACIO COSTA 21")
            if " - " in evento_limpio:
                parts = evento_limpio.split(" - ")
                if len(parts) >= 4:
                    # El venue está en la última parte
                    venue = parts[-1].strip()
                    return venue
                elif len(parts) >= 3:
                    # Si hay 3 partes, el venue podría estar en la última
                    venue = parts[-1].strip()
                    return venue
            
            # Dividir por " EN " para separar artista de venue (formato anterior)
            if " EN " in evento_limpio:
                parts = evento_limpio.split(" EN ")
                if len(parts) >= 2:
                    venue_part = parts[1].strip()
                    return venue_part
                else:
                    return "Teleticket"
            else:
                # Si no hay " EN ", usar un venue por defecto
                return "Teleticket"
                
        except Exception as e:
            logger.error(f"Error extrayendo nombre del venue: {str(e)}")
            return "Teleticket"
    
    def extract_event_date(self, event_data):
        """
        Extrae la fecha del evento de los datos extraídos
        
        Args:
            event_data (dict): Datos del evento extraído
            
        Returns:
            datetime: Fecha del evento o None si no se encuentra
        """
        try:
            for item in event_data.get("datos", []):
                if item.get("fecha_evento") and item.get("fecha_evento") != "Total":
                    fecha_str = item.get("fecha_evento")
                    # Formato: "12/08/2025 20:00"
                    if "/" in fecha_str and ":" in fecha_str:
                        # Convertir formato DD/MM/YYYY HH:MM a datetime
                        from datetime import datetime
                        fecha_obj = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M")
                        return fecha_obj
            return None
        except Exception as e:
            logger.error(f"Error extrayendo fecha del evento: {str(e)}")
            return None
    
    def extract_city_name(self, venue_name):
        """
        Extrae el nombre de la ciudad del venue
        
        Args:
            venue_name (str): Nombre del venue (ej: "JARDIN DE LA CERVEZA - AREQUIPA")
            
        Returns:
            str: Nombre de la ciudad (ej: "Arequipa, Perú")
        """
        try:
            if not venue_name:
                return "Lima, Perú"
            
            # Buscar patrones de ciudades conocidas
            venue_upper = venue_name.upper()
            
            if "AREQUIPA" in venue_upper:
                return "Arequipa, Perú"
            elif "LIMA" in venue_upper:
                return "Lima, Perú"
            elif "CUSCO" in venue_upper:
                return "Cusco, Perú"
            elif "TRUJILLO" in venue_upper:
                return "Trujillo, Perú"
            elif "CHICLAYO" in venue_upper:
                return "Chiclayo, Perú"
            else:
                # Por defecto, Lima
                return "Lima, Perú"
                
        except Exception as e:
            logger.error(f"Error extrayendo ciudad: {str(e)}")
            return "Lima, Perú"
    
    def calculate_event_totals(self, event_data):
        """
        Calcula los totales del evento basado en los datos extraídos
        
        Args:
            event_data (dict): Datos del evento extraído
            
        Returns:
            dict: Diccionario con los totales calculados
        """
        try:
            totals = {
                "total_vendido": 0,
                "total_recaudado": 0,
                "total_capacidad": 0,
                "total_disponible": 0,
                "porcentaje_ocupacion": 0.0,
                "total_eventos": 0
            }
            
            # Buscar datos de cantidad y monto en los datos extraídos
            cantidad_data = []
            monto_data = []
            
            for item in event_data.get("datos", []):
                if item.get("tabla_tipo") == "cantidad":
                    cantidad_data.append(item)
                elif item.get("tabla_tipo") == "monto":
                    monto_data.append(item)
            
            # Procesar datos de cantidad
            for item in cantidad_data:
                if item.get("es_total"):
                    total_str = item.get("total", "0")
                    if total_str and total_str != "0":
                        # Limpiar formato peruano (puntos como separadores de miles)
                        if '.' in total_str and ',' not in total_str:
                            total_str = total_str.replace('.', '')
                        elif ',' in total_str and '.' in total_str:
                            total_str = total_str.replace(',', '').replace('.', '')
                        elif ',' in total_str:
                            total_str = total_str.replace(',', '.')
                        
                        try:
                            totals["total_vendido"] = int(float(total_str))
                        except:
                            totals["total_vendido"] = 0
            
            # Procesar datos de monto
            for item in monto_data:
                if item.get("es_total"):
                    total_str = item.get("total", "0")
                    if total_str and total_str != "0":
                        # Limpiar formato peruano (puntos como separadores de miles)
                        if '.' in total_str and ',' not in total_str:
                            total_str = total_str.replace('.', '')
                        elif ',' in total_str and '.' in total_str:
                            total_str = total_str.replace(',', '').replace('.', '')
                        elif ',' in total_str:
                            total_str = total_str.replace(',', '.')
                        
                        try:
                            totals["total_recaudado"] = int(float(total_str))
                        except:
                            totals["total_recaudado"] = 0
            
            # Calcular capacidad total (suma de todas las categorías)
            for item in cantidad_data:
                if not item.get("es_total"):
                    total_str = item.get("total", "0")
                    if total_str and total_str != "0":
                        # Limpiar formato peruano
                        if '.' in total_str and ',' not in total_str:
                            total_str = total_str.replace('.', '')
                        elif ',' in total_str and '.' in total_str:
                            total_str = total_str.replace(',', '').replace('.', '')
                        elif ',' in total_str:
                            total_str = total_str.replace(',', '.')
                        
                        try:
                            totals["total_capacidad"] += int(float(total_str))
                        except:
                            continue
            
            # Si no hay capacidad calculada, usar el total vendido
            if totals["total_capacidad"] == 0:
                totals["total_capacidad"] = totals["total_vendido"]
            
            # Calcular disponibles y porcentaje de ocupación
            totals["total_disponible"] = totals["total_capacidad"] - totals["total_vendido"]
            
            if totals["total_capacidad"] > 0:
                totals["porcentaje_ocupacion"] = round((totals["total_vendido"] / totals["total_capacidad"]) * 100, 2)
            
            totals["total_eventos"] = 1
            
            logger.info(f"📊 Totales calculados para evento:")
            logger.info(f"  📊 Capacidad: {totals['total_capacidad']}")
            logger.info(f"  🎫 Vendido: {totals['total_vendido']}")
            logger.info(f"  🆓 Disponible: {totals['total_disponible']}")
            logger.info(f"  💰 Recaudado: ${totals['total_recaudado']:,}")
            logger.info(f"  📈 Ocupación: {totals['porcentaje_ocupacion']}%")
            logger.info(f"  🎭 Eventos: {totals['total_eventos']}")
            
            return totals
            
        except Exception as e:
            logger.error(f"Error calculando totales del evento: {str(e)}")
            return {
                "total_vendido": 0,
                "total_recaudado": 0,
                "total_capacidad": 0,
                "total_disponible": 0,
                "porcentaje_ocupacion": 0.0,
                "total_eventos": 0
            }
    
    def save_to_database(self, event_data):
        """
        FUNCIÓN DESHABILITADA - No usamos raw_data
        
        Args:
            event_data (dict): Datos del evento extraído
            
        Returns:
            bool: False (función deshabilitada)
        """
        try:
            # FUNCIÓN DESHABILITADA - No usamos raw_data
            logger.info("🚫 FUNCIÓN DESHABILITADA - No usamos raw_data")
            return False
        except Exception as e:
            logger.error(f"❌ Error en función deshabilitada: {str(e)}")
                return False
            
    def validate_daily_sales_data(self, event_data, show_id):
        """
        VALIDACIÓN: Muestra exactamente qué datos se van a insertar/actualizar
        sin guardarlos realmente en la base de datos
        
        Args:
            event_data (dict): Datos del evento extraído
            show_id (str): ID del show en la tabla shows
            
        Returns:
            dict: Resumen de validación con todos los datos que se procesarían
        """
        try:
            if not event_data.get('ventas_diarias') or not event_data.get('montos_diarias'):
                logger.warning("⚠️ No hay datos de ventas o montos diarios para validar")
                logger.warning(f"   ventas_diarias: {len(event_data.get('ventas_diarias', [])) if event_data.get('ventas_diarias') else 'None/Empty'}")
                logger.warning(f"   montos_diarios: {len(event_data.get('montos_diarios', [])) if event_data.get('montos_diarios') else 'None/Empty'}")
                return None
            
            logger.info("🔍 VALIDANDO DATOS DE VENTAS DIARIAS (SIN GUARDAR)...")
            
            # Obtener capacidad total del show
            cursor = self.db_connection.cursor()
            capacity_query = "SELECT capacidad_total FROM shows WHERE id = %s"
            cursor.execute(capacity_query, (show_id,))
            capacity_result = cursor.fetchone()
            capacidad_total = capacity_result[0] if capacity_result else 0
            
            logger.info(f"🏟️ Capacidad total del show: {capacidad_total}")
            
            # Combinar ventas y montos por fecha
            ventas_dict = {v['fecha']: v['ventas'] for v in event_data['ventas_diarias']}
            montos_dict = {m['fecha']: m['monto'] for m in event_data['montos_diarias']}
            
            # Obtener todas las fechas únicas
            todas_las_fechas = set(ventas_dict.keys()) | set(montos_dict.keys())
            
            # Calcular totales acumulados
            venta_total_acumulada = 0
            recaudacion_total_ars = 0
            
            # Ordenar fechas cronológicamente
            from datetime import datetime
            fechas_ordenadas = sorted(todas_las_fechas, key=lambda x: datetime.strptime(x, "%d-%m-%Y"))
            
            registros_a_procesar = []
            registros_a_actualizar = 0
            registros_a_insertar = 0
            
            logger.info("📊 RESUMEN DE DATOS A PROCESAR:")
            logger.info("=" * 80)
            
            for fecha_str in fechas_ordenadas:
                try:
                    # Convertir fecha de DD-MM-YYYY a YYYY-MM-DD
                    fecha_obj = datetime.strptime(fecha_str, "%d-%m-%Y")
                    fecha_venta = fecha_obj.strftime("%Y-%m-%d")
                    
                    # Obtener datos del día
                    venta_diaria = ventas_dict.get(fecha_str, 0)
                    monto_diario_ars = int(montos_dict.get(fecha_str, 0))
                    
                    # Acumular totales
                    venta_total_acumulada += venta_diaria
                    recaudacion_total_ars += monto_diario_ars
                    
                    # Calcular métricas
                    tickets_disponibles = max(0, capacidad_total - venta_total_acumulada)
                    porcentaje_ocupacion = (venta_total_acumulada / capacidad_total * 100) if capacidad_total > 0 else 0
                    precio_promedio_ars = int(monto_diario_ars / venta_diaria) if venta_diaria > 0 else 0
                    
                    # Verificar si ya existe el registro
                    check_query = """
                        SELECT id, venta_diaria, monto_diario_ars, venta_total_acumulada, recaudacion_total_ars
                        FROM daily_sales 
                        WHERE show_id = %s AND fecha_venta = %s
                    """
                    cursor.execute(check_query, (show_id, fecha_venta))
                    existing_record = cursor.fetchone()
                    
                    registro_info = {
                        'fecha': fecha_venta,
                        'fecha_original': fecha_str,
                        'venta_diaria': venta_diaria,
                        'monto_diario_ars': monto_diario_ars,
                        'venta_total_acumulada': venta_total_acumulada,
                        'recaudacion_total_ars': recaudacion_total_ars,
                        'tickets_disponibles': tickets_disponibles,
                        'porcentaje_ocupacion': porcentaje_ocupacion,
                        'precio_promedio_ars': precio_promedio_ars,
                        'existe': existing_record is not None,
                        'accion': 'ACTUALIZAR' if existing_record else 'INSERTAR'
                    }
                    
                    if existing_record:
                        registro_info['datos_actuales'] = {
                            'venta_diaria_actual': existing_record[1],
                            'monto_diario_ars_actual': existing_record[2],
                            'venta_total_acumulada_actual': existing_record[3],
                            'recaudacion_total_ars_actual': existing_record[4]
                        }
                        registros_a_actualizar += 1
                    else:
                        registros_a_insertar += 1
                    
                    registros_a_procesar.append(registro_info)
                    
                    # Mostrar resumen del día
                    accion_emoji = "🔄" if existing_record else "➕"
                    logger.info(f"{accion_emoji} {fecha_venta} ({registro_info['accion']}):")
                    logger.info(f"   📊 Ventas: {venta_diaria} | 💰 Monto: ${monto_diario_ars:,} ARS")
                    logger.info(f"   📈 Acumulado: {venta_total_acumulada} ventas | ${recaudacion_total_ars:,} ARS")
                    logger.info(f"   🎫 Disponibles: {tickets_disponibles} | 📊 Ocupación: {porcentaje_ocupacion:.1f}%")
                    
                    if existing_record:
                        logger.info(f"   🔄 DATOS ACTUALES:")
                        logger.info(f"      Ventas: {existing_record[1]} → {venta_diaria}")
                        logger.info(f"      Monto: ${existing_record[2]:,} → ${monto_diario_ars:,}")
                    
                except Exception as e:
                    logger.error(f"❌ Error validando fecha {fecha_str}: {str(e)}")
                    continue
            
            # Resumen final
            logger.info("=" * 80)
            logger.info("📋 RESUMEN DE VALIDACIÓN:")
            logger.info(f"  📊 Total de registros a procesar: {len(registros_a_procesar)}")
            logger.info(f"  🔄 Registros a actualizar: {registros_a_actualizar}")
            logger.info(f"  ➕ Registros a insertar: {registros_a_insertar}")
            logger.info(f"  🎫 Venta total acumulada final: {venta_total_acumulada}")
            logger.info(f"  💰 Recaudación total final: ${recaudacion_total_ars:,} ARS")
            logger.info(f"  📈 Ocupación final: {porcentaje_ocupacion:.1f}%")
            logger.info(f"  🏟️ Capacidad total: {capacidad_total}")
            logger.info("=" * 80)
            
            return {
                'registros': registros_a_procesar,
                'resumen': {
                    'total_registros': len(registros_a_procesar),
                    'a_actualizar': registros_a_actualizar,
                    'a_insertar': registros_a_insertar,
                    'venta_total_acumulada': venta_total_acumulada,
                    'recaudacion_total_ars': recaudacion_total_ars,
                    'ocupacion_final': porcentaje_ocupacion,
                    'capacidad_total': capacidad_total
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error validando datos: {str(e)}")
            return None
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def save_daily_sales_to_database(self, event_data, show_id):
        """
        Guarda/actualiza las ventas diarias en la tabla daily_sales
        
        Args:
            event_data (dict): Datos del evento extraído
            show_id (str): ID del show en la tabla shows
            
        Returns:
            bool: True si se guardó exitosamente, False en caso contrario
        """
        try:
            ventas_diarias = event_data.get('ventas_diarias', [])
            montos_diarios = event_data.get('montos_diarios', [])
            
            if not ventas_diarias or not montos_diarios or len(ventas_diarias) == 0 or len(montos_diarios) == 0:
                logger.warning("⚠️ No hay datos de ventas o montos diarios para guardar")
                return False
            
            logger.info("💾 GUARDANDO VENTAS DIARIAS EN LA BASE DE DATOS...")
            
            # Obtener capacidad total del show
            cursor = self.db_connection.cursor()
            capacity_query = "SELECT capacidad_total FROM shows WHERE id = %s"
            cursor.execute(capacity_query, (show_id,))
            capacity_result = cursor.fetchone()
            capacidad_total = capacity_result[0] if capacity_result else 0
            
            logger.info(f"🏟️ Capacidad total del show: {capacidad_total}")
            
            # Combinar ventas y montos por fecha
            ventas_dict = {v['fecha']: v['ventas'] for v in event_data['ventas_diarias']}
            montos_dict = {m['fecha']: m['monto'] for m in event_data['montos_diarias']}
            
            # Obtener todas las fechas únicas
            todas_las_fechas = set(ventas_dict.keys()) | set(montos_dict.keys())
            
            # Calcular totales acumulados
            venta_total_acumulada = 0
            recaudacion_total_ars = 0
            
            # Ordenar fechas cronológicamente
            from datetime import datetime
            fechas_ordenadas = sorted(todas_las_fechas, key=lambda x: datetime.strptime(x, "%d-%m-%Y"))
            
            registros_procesados = 0
            registros_actualizados = 0
            registros_insertados = 0
            
            for fecha_str in fechas_ordenadas:
                try:
                    # Convertir fecha de DD-MM-YYYY a YYYY-MM-DD
                    fecha_obj = datetime.strptime(fecha_str, "%d-%m-%Y")
                    fecha_venta = fecha_obj.strftime("%Y-%m-%d")
                    
                    # Obtener datos del día
                    venta_diaria = ventas_dict.get(fecha_str, 0)
                    monto_diario_ars = int(montos_dict.get(fecha_str, 0))
                    
                    # Acumular totales
                    venta_total_acumulada += venta_diaria
                    recaudacion_total_ars += monto_diario_ars
                    
                    # Calcular métricas
                    tickets_disponibles = max(0, capacidad_total - venta_total_acumulada)
                    porcentaje_ocupacion = (venta_total_acumulada / capacidad_total * 100) if capacidad_total > 0 else 0
                    precio_promedio_ars = int(monto_diario_ars / venta_diaria) if venta_diaria > 0 else 0
                    
                    # Verificar si ya existe el registro
                    check_query = """
                        SELECT id FROM daily_sales 
                        WHERE show_id = %s AND fecha_venta = %s
                    """
                    cursor.execute(check_query, (show_id, fecha_venta))
                    existing_record = cursor.fetchone()
                    
                    if existing_record:
                        # ACTUALIZAR registro existente
                        update_query = """
                            UPDATE daily_sales SET
                                venta_diaria = %s,
                                monto_diario_ars = %s,
                                venta_total_acumulada = %s,
                                recaudacion_total_ars = %s,
                                tickets_disponibles = %s,
                                porcentaje_ocupacion = %s,
                                precio_promedio_ars = %s,
                                fecha_extraccion = NOW(),
                                updated_at = NOW()
                            WHERE show_id = %s AND fecha_venta = %s
                        """
                        cursor.execute(update_query, (
                            venta_diaria, monto_diario_ars, venta_total_acumulada,
                            recaudacion_total_ars, tickets_disponibles, porcentaje_ocupacion,
                            precio_promedio_ars, show_id, fecha_venta
                        ))
                        registros_actualizados += 1
                        logger.info(f"🔄 Actualizado: {fecha_venta} - {venta_diaria} ventas, ${monto_diario_ars:,} ARS")
                        
                    else:
                        # INSERTAR nuevo registro
            insert_query = """
                            INSERT INTO daily_sales (
                                show_id, fecha_venta, venta_diaria, monto_diario_ars,
                                venta_total_acumulada, recaudacion_total_ars, tickets_disponibles,
                                porcentaje_ocupacion, precio_promedio_ars, ticketera, fecha_extraccion
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                            )
                        """
                        cursor.execute(insert_query, (
                            show_id, fecha_venta, venta_diaria, monto_diario_ars,
                            venta_total_acumulada, recaudacion_total_ars, tickets_disponibles,
                            porcentaje_ocupacion, precio_promedio_ars, 'teleticket'
                        ))
                        registros_insertados += 1
                        logger.info(f"➕ Insertado: {fecha_venta} - {venta_diaria} ventas, ${monto_diario_ars:,} ARS")
                    
                    registros_procesados += 1
                    
                except Exception as e:
                    logger.error(f"❌ Error procesando fecha {fecha_str}: {str(e)}")
                    continue
            
            # Confirmar transacción
            self.db_connection.commit()
            
            logger.info(f"✅ VENTAS DIARIAS GUARDADAS EXITOSAMENTE:")
            logger.info(f"  📊 Registros procesados: {registros_procesados}")
            logger.info(f"  🔄 Registros actualizados: {registros_actualizados}")
            logger.info(f"  ➕ Registros insertados: {registros_insertados}")
            logger.info(f"  🎫 Venta total acumulada: {venta_total_acumulada}")
            logger.info(f"  💰 Recaudación total: ${recaudacion_total_ars:,} ARS")
            logger.info(f"  📈 Ocupación final: {porcentaje_ocupacion:.1f}%")
            
                return True
                
        except Exception as e:
            logger.error(f"❌ Error guardando ventas diarias: {str(e)}")
                self.db_connection.rollback()
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def setup_driver(self):
        """Configura el driver de Chrome con las opciones necesarias"""
        try:
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument("--headless")
            
            # Opciones básicas para Windows
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            
            # Configurar user agent
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Configurar carpeta de descargas
            chrome_options.add_experimental_option("prefs", {
                "download.default_directory": os.path.abspath(self.download_folder),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            })
            
            # Intentar configurar el driver de forma simple
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
            except Exception as e:
                logger.warning(f"Error con configuración simple: {str(e)}")
                # Intentar con webdriver-manager como respaldo
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            logger.info("Driver de Chrome configurado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error al configurar el driver: {str(e)}")
            return False
    
    def navigate_to_login(self):
        """Navega a la página de login de Teleticket"""
        try:
            logger.info(f"Navegando a: {self.base_url}")
            self.driver.get(self.base_url)
            
            # Esperar a que la página cargue completamente
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            logger.info("Página de login cargada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error navegando a la página de login: {str(e)}")
            return False
    
    def try_login_with_credential(self, username, password):
        """Intenta hacer login con una credencial específica"""
        try:
            logger.info(f"Intentando login con usuario: {username}")
            
            # Esperar a que los campos de login estén disponibles
            username_field = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "UserName"))
            )
            
            password_field = self.driver.find_element(By.ID, "Password")
            
            # Limpiar campos y escribir credenciales
            username_field.clear()
            username_field.send_keys(username)
            logger.info("Usuario ingresado")
            
            password_field.clear()
            password_field.send_keys(password)
            logger.info("Contraseña ingresada")
            
            # Hacer clic en el botón de login
            login_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            login_button.click()
            logger.info("Botón de login clickeado")
            
            # Esperar a que se complete el login (redirección o cambio de página)
            time.sleep(3)
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            if "LogOn" not in current_url:
                logger.info(f"Login exitoso con usuario: {username}")
                return True
            else:
                # Verificar si hay mensaje de error
                try:
                    error_element = self.driver.find_element(By.CLASS_NAME, "validation-summary-valid")
                    if error_element.is_displayed():
                        error_text = error_element.text
                        logger.warning(f"Error en login con {username}: {error_text}")
                        return False
                except:
                    pass
                
                logger.warning(f"Login falló con usuario: {username} - aún en página de login")
                return False
                
        except Exception as e:
            logger.error(f"Error durante el login con {username}: {str(e)}")
            return False

    def login_with_specific_credential(self, credential_index):
        """Realiza el login con una credencial específica"""
        if credential_index >= len(self.credentials):
            logger.error(f"Índice de credencial inválido: {credential_index}")
            return False
            
        credential = self.credentials[credential_index]
        username = credential["username"]
        password = credential["password"]
        
        logger.info(f"Intentando login con credencial {credential_index + 1}/{len(self.credentials)}: {username}")
        
        # Intentar login con esta credencial
        if self.try_login_with_credential(username, password):
            self.current_credential_index = credential_index
            logger.info(f"Login exitoso con credencial: {username}")
            return True
        else:
            logger.warning(f"Login falló con credencial: {username}")
            return False
    
    def get_current_credential_info(self):
        """Obtiene información sobre la credencial actualmente en uso"""
        if self.current_credential_index < len(self.credentials):
            current_cred = self.credentials[self.current_credential_index]
            return f"{current_cred['username']}"
        return "Ninguna"

    def get_page_info(self):
        """Obtiene información básica de la página actual"""
        try:
            logger.info("Obteniendo información de la página...")
            
            # Título de la página
            title = self.driver.title
            logger.info(f"Título de la página: {title}")
            
            # URL actual
            current_url = self.driver.current_url
            logger.info(f"URL actual: {current_url}")
            
            # Credencial utilizada
            current_user = self.get_current_credential_info()
            logger.info(f"Usuario logueado: {current_user}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error obteniendo información de la página: {str(e)}")
            return False
    
    def navigate_to_evolucion_ventas(self):
        """Navega directamente a la página de Evolución Ventas"""
        try:
            logger.info("Navegando directamente a Evolución Ventas...")
            
            # URL específica de Evolución Ventas
            evolucion_ventas_url = "https://backoffice.teleticket.com.pe/Reporte?opc=5&title=5.%20Evoluci%C3%B3n%20Ventas"
            
            logger.info(f"Accediendo a: {evolucion_ventas_url}")
            self.driver.get(evolucion_ventas_url)
            
            # Esperar a que se cargue la página
            time.sleep(5)
            
            # Verificar que se cargó correctamente
            current_url = self.driver.current_url
            if "opc=5" in current_url and "Evoluci" in current_url:
                logger.info("✅ Navegación exitosa a Evolución Ventas")
                logger.info(f"URL actual: {current_url}")
                return True
            else:
                logger.warning(f"URL actual no corresponde a Evolución Ventas: {current_url}")
                return False
                
        except Exception as e:
            logger.error(f"Error navegando a Evolución Ventas: {str(e)}")
            return False
    
    def get_evolucion_ventas_info(self):
        """Obtiene información de la página de Evolución Ventas"""
        try:
            logger.info("Obteniendo información de Evolución Ventas...")
            
            # Título de la página
            title = self.driver.title
            logger.info(f"Título de la página: {title}")
            
            # URL actual
            current_url = self.driver.current_url
            logger.info(f"URL actual: {current_url}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error obteniendo información de Evolución Ventas: {str(e)}")
            return False
    
    def extract_daily_amounts(self):
        """Extrae los montos diarios de la tabla de montos"""
        try:
            logger.info("💰 EXTRAYENDO MONTOS DIARIOS DE LA TABLA DE MONTOS...")
            
            # Buscar la tabla de montos
            montos_table = None
            try:
                # Buscar por ID
                montos_table = self.driver.find_element(By.ID, "Montos")
            except:
                try:
                    # Buscar por clase que contenga "montos"
                    montos_table = self.driver.find_element(By.CSS_SELECTOR, "table[id*='Montos'], table[class*='montos']")
                except:
                    # Buscar cualquier tabla que contenga montos en los headers
                    tables = self.driver.find_elements(By.TAG_NAME, "table")
                    for table in tables:
                        try:
                            # Verificar si es la tabla de montos por el contenido
                            table_html = table.get_attribute('outerHTML')
                            if 'Montos' in table_html and '481,994.00' in table_html:
                                montos_table = table
                                break
                        except:
                            continue
            
            if not montos_table:
                logger.warning("❌ No se encontró la tabla de montos")
                return None
            
            logger.info("✅ Tabla de montos encontrada")
            
            # Extraer headers (fechas)
            headers = montos_table.find_elements(By.CSS_SELECTOR, "thead th, thead td")
            if not headers:
                # Si no hay thead, buscar en la primera fila
                first_row = montos_table.find_element(By.CSS_SELECTOR, "tr:first-child")
                headers = first_row.find_elements(By.TAG_NAME, "th, td")
            
            dates = []
            for i, header in enumerate(headers):
                if i == 0:  # Saltar la primera columna (nombre del evento)
                    continue
                date_text = header.text.strip()
                if date_text and ("2025" in date_text or "-" in date_text):
                    dates.append(date_text)
            
            logger.info(f"📅 Fechas encontradas en montos: {len(dates)} días")
            
            # Extraer filas de datos
            rows = montos_table.find_elements(By.CSS_SELECTOR, "tbody tr, tr:not(:first-child)")
            daily_amounts = []
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) > 1:
                    row_name = cells[0].text.strip()
                    
                    # Solo procesar la fila "Total" o la primera fila con datos
                    if "Total" in row_name or not daily_amounts:
                        logger.info(f"💰 Procesando fila de montos: {row_name}")
                        
                        for i, cell in enumerate(cells[1:], 1):  # Saltar primera columna
                            if i-1 < len(dates):
                                try:
                                    # Limpiar el texto del monto (quitar comas y convertir a float)
                                    amount_text = cell.text.strip().replace(',', '')
                                    amount_value = float(amount_text) if amount_text.replace('.', '').isdigit() else 0.0
                                    daily_amounts.append({
                                        "fecha": dates[i-1],
                                        "monto": amount_value,
                                        "fila": row_name
                                    })
                                except ValueError:
                                    daily_amounts.append({
                                        "fecha": dates[i-1],
                                        "monto": 0.0,
                                        "fila": row_name
                                    })
                        break  # Solo procesar la primera fila válida
            
            logger.info(f"✅ Montos diarios extraídos: {len(daily_amounts)} registros")
            
            # Mostrar resumen de los montos diarios
            if daily_amounts:
                logger.info("💰 RESUMEN DE MONTOS DIARIOS:")
                total_montos = sum(day['monto'] for day in daily_amounts)
                logger.info(f"  💰 Total de montos: ${total_montos:,.2f}")
                logger.info(f"  📅 Días con datos: {len([d for d in daily_amounts if d['monto'] > 0])}")
                
                # Mostrar los primeros 5 días
                for i, day in enumerate(daily_amounts[:5]):
                    logger.info(f"  📅 {day['fecha']}: ${day['monto']:,.2f}")
                
                if len(daily_amounts) > 5:
                    logger.info(f"  ... y {len(daily_amounts) - 5} días más")
            
            return daily_amounts
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo montos diarios: {str(e)}")
            return None
    
    def extract_event_and_date_data(self):
        """Extrae específicamente el evento y fecha de los elementos clave"""
        try:
            logger.info("🎯 EXTRAYENDO DATOS ESPECÍFICOS DEL EVENTO Y FECHA...")
            
            # Esperar a que la página esté completamente cargada
            time.sleep(3)
            
            event_data = {
                "evento_nombre": "",
                "evento_fecha": "",
                "evento_id": "",
                "venue": "",
                "artista": "",
                "timestamp": datetime.now().isoformat()
            }
            
            # 1. EXTRAER NOMBRE DEL EVENTO DEL INPUT eventSelect
            try:
                event_select = self.driver.find_element(By.ID, "eventSelect")
                evento_nombre = event_select.get_attribute("value") or event_select.get_attribute("aria-label") or ""
                event_data["evento_nombre"] = evento_nombre
                logger.info(f"🎭 Evento encontrado: {evento_nombre}")
            except Exception as e:
                logger.warning(f"No se pudo extraer el nombre del evento: {str(e)}")
            
            # 2. EXTRAER FECHA DEL SELECT calendarioId
            try:
                calendario_select = self.driver.find_element(By.ID, "calendarioId")
                selected_option = calendario_select.find_element(By.CSS_SELECTOR, "option[selected], option:first-child")
                fecha_texto = selected_option.text.strip()
                fecha_value = selected_option.get_attribute("value")
                event_data["evento_fecha"] = fecha_texto
                event_data["evento_id"] = fecha_value
                logger.info(f"📅 Fecha encontrada: {fecha_texto}")
                logger.info(f"🆔 ID del evento: {fecha_value}")
            except Exception as e:
                logger.warning(f"No se pudo extraer la fecha: {str(e)}")
            
            # 3. EXTRAER INFORMACIÓN ADICIONAL DEL NOMBRE DEL EVENTO
            if event_data["evento_nombre"]:
                # Extraer artista del nombre del evento
                artista = self.extract_artist_name(event_data["evento_nombre"])
                event_data["artista"] = artista
                
                # Extraer venue del nombre del evento
                venue = self.extract_venue_name(event_data["evento_nombre"])
                event_data["venue"] = venue
                
                logger.info(f"🎤 Artista extraído: {artista}")
                logger.info(f"🏟️ Venue extraído: {venue}")
            
            # 4. EXTRAER VENTAS DIARIAS DE LA TABLA DE CANTIDADES
            daily_sales = self.extract_daily_sales()
            if daily_sales:
                event_data['ventas_diarias'] = daily_sales
                logger.info(f"📊 Ventas diarias extraídas: {len(daily_sales)} días")
            
            # 5. EXTRAER MONTOS DIARIOS DE LA TABLA DE MONTOS
            daily_amounts = self.extract_daily_amounts()
            if daily_amounts:
                event_data['montos_diarios'] = daily_amounts
                logger.info(f"💰 Montos diarios extraídos: {len(daily_amounts)} días")
            
            return event_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos del evento: {str(e)}")
            return None
    
    def extract_daily_sales(self):
        """Extrae las ventas diarias de la tabla de cantidades"""
        try:
            logger.info("📊 EXTRAYENDO VENTAS DIARIAS DE LA TABLA DE CANTIDADES...")
            
            # Buscar la tabla de cantidades
            cantidades_table = None
            try:
                # Buscar por ID
                cantidades_table = self.driver.find_element(By.ID, "Cantidades")
            except:
                try:
                    # Buscar por clase que contenga "cantidades"
                    cantidades_table = self.driver.find_element(By.CSS_SELECTOR, "table[id*='Cantidades'], table[class*='cantidades']")
                except:
                    # Buscar cualquier tabla que contenga fechas en los headers
                    tables = self.driver.find_elements(By.TAG_NAME, "table")
                    for table in tables:
                        try:
                            headers = table.find_elements(By.CSS_SELECTOR, "th, thead td")
                            if headers and any("2025" in header.text for header in headers):
                                cantidades_table = table
                                break
                        except:
                            continue
            
            if not cantidades_table:
                logger.warning("❌ No se encontró la tabla de cantidades")
                return None
            
            logger.info("✅ Tabla de cantidades encontrada")
            
            # Extraer headers (fechas)
            headers = cantidades_table.find_elements(By.CSS_SELECTOR, "thead th, thead td")
            if not headers:
                # Si no hay thead, buscar en la primera fila
                first_row = cantidades_table.find_element(By.CSS_SELECTOR, "tr:first-child")
                headers = first_row.find_elements(By.TAG_NAME, "th, td")
            
            dates = []
            for i, header in enumerate(headers):
                if i == 0:  # Saltar la primera columna (nombre del evento)
                    continue
                date_text = header.text.strip()
                if date_text and ("2025" in date_text or "-" in date_text):
                    dates.append(date_text)
            
            logger.info(f"📅 Fechas encontradas: {len(dates)} días")
            
            # Extraer filas de datos
            rows = cantidades_table.find_elements(By.CSS_SELECTOR, "tbody tr, tr:not(:first-child)")
            daily_sales = []
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) > 1:
                    row_name = cells[0].text.strip()
                    
                    # Solo procesar la fila "Total" o la primera fila con datos
                    if "Total" in row_name or not daily_sales:
                        logger.info(f"📊 Procesando fila: {row_name}")
                        
                        for i, cell in enumerate(cells[1:], 1):  # Saltar primera columna
                            if i-1 < len(dates):
                                try:
                                    sales_value = int(cell.text.strip()) if cell.text.strip().isdigit() else 0
                                    daily_sales.append({
                                        "fecha": dates[i-1],
                                        "ventas": sales_value,
                                        "fila": row_name
                                    })
                                except ValueError:
                                    daily_sales.append({
                                        "fecha": dates[i-1],
                                        "ventas": 0,
                                        "fila": row_name
                                    })
                        break  # Solo procesar la primera fila válida
            
            logger.info(f"✅ Ventas diarias extraídas: {len(daily_sales)} registros")
            
            # Mostrar resumen de las ventas diarias
            if daily_sales:
                logger.info("📈 RESUMEN DE VENTAS DIARIAS:")
                total_ventas = sum(day['ventas'] for day in daily_sales)
                logger.info(f"  📊 Total de ventas: {total_ventas}")
                logger.info(f"  📅 Días con datos: {len([d for d in daily_sales if d['ventas'] > 0])}")
                
                # Mostrar los primeros 5 días
                for i, day in enumerate(daily_sales[:5]):
                    logger.info(f"  📅 {day['fecha']}: {day['ventas']} ventas")
                
                if len(daily_sales) > 5:
                    logger.info(f"  ... y {len(daily_sales) - 5} días más")
            
            return daily_sales
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo ventas diarias: {str(e)}")
            return None
    
    def match_with_database(self, event_data):
        """Compara los datos extraídos con los datos de la base de datos"""
        try:
            if not event_data or not self.db_connection:
                logger.warning("No hay datos del evento o conexión a BD para hacer matching")
                return None
            
            logger.info("🔍 BUSCANDO MATCHING EN LA TABLA SHOWS...")
            
            # Buscar en la tabla shows por artista y fecha
            cursor = self.db_connection.cursor()
            
            # Convertir la fecha del formato "2025-11-30 21:00 - Domingo" a datetime
            fecha_str = event_data["evento_fecha"]
            if " - " in fecha_str:
                fecha_part = fecha_str.split(" - ")[0]  # "2025-11-30 21:00"
                try:
                    from datetime import datetime
                    fecha_obj = datetime.strptime(fecha_part, "%Y-%m-%d %H:%M")
                    
                    # Buscar en la tabla shows (solo por artista y fecha)
                    query = """
                        SELECT 
                            id, ticketera, artista, venue, fecha_show, created_at
                        FROM shows 
                        WHERE ticketera = 'teleticket' 
                        AND artista = %s 
                        AND DATE(fecha_show) = %s
                        ORDER BY created_at DESC
                        LIMIT 5;
                    """
                    
                    cursor.execute(query, (event_data["artista"], fecha_obj.date()))
                    results = cursor.fetchall()
                    
                    if results:
                        logger.info(f"✅ MATCHING ENCONTRADO: {len(results)} show(s) en la tabla shows")
                        
                        for i, result in enumerate(results):
                            logger.info(f"📊 Show {i+1}:")
                            logger.info(f"  🆔 ID: {result[0]}")
                            logger.info(f"  🎤 Artista: {result[2]}")
                            logger.info(f"  🏟️ Venue: {result[3]}")
                            logger.info(f"  📅 Fecha Show: {result[4]}")
                            logger.info(f"  ⏰ Creado: {result[5]}")
                        
                        cursor.close()
                        return results
                    else:
                        logger.info("❌ NO SE ENCONTRÓ MATCHING EN LA TABLA SHOWS")
                        logger.info(f"Buscando: Artista='{event_data['artista']}', Fecha='{fecha_obj.date()}' (sin venue)")
                        logger.info("💡 Este sería un nuevo show para agregar a la tabla shows")
                        
                        # Buscar shows similares en la tabla shows
                        similar_query = """
                            SELECT artista, venue, fecha_show, created_at
                            FROM shows 
                            WHERE ticketera = 'teleticket' 
                            AND (artista ILIKE %s OR artista ILIKE %s)
                            ORDER BY created_at DESC
                            LIMIT 5;
                        """
                        
                        cursor.execute(similar_query, (f"%{event_data['artista']}%", f"%{event_data['artista'].split()[0]}%"))
                        similar_results = cursor.fetchall()
                        
                        if similar_results:
                            logger.info("🔍 SHOWS SIMILARES ENCONTRADOS EN LA TABLA SHOWS:")
                            for result in similar_results:
                                logger.info(f"  🎤 {result[0]} | 🏟️ {result[1]} | 📅 {result[2]} | ⏰ {result[3]}")
                        
                        cursor.close()
                        return None
                        
                except Exception as e:
                    logger.error(f"Error procesando fecha: {str(e)}")
                    cursor.close()
                    return None
            else:
                logger.warning("Formato de fecha no reconocido")
                cursor.close()
                return None
                
        except Exception as e:
            logger.error(f"Error haciendo matching con la base de datos: {str(e)}")
            if self.db_connection:
                try:
                    cursor.close()
                except:
                    pass
            return None
    
    def extract_all_page_data(self):
        """Extrae TODOS los datos visibles en la página de Evolución Ventas"""
        try:
            logger.info("🔍 EXTRAYENDO TODOS LOS DATOS DE LA PÁGINA...")
            
            # Esperar a que la página esté completamente cargada
            time.sleep(3)
            
            # Obtener información básica de la página
            page_info = {
                "title": self.driver.title,
                "url": self.driver.current_url,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"📄 Título de la página: {page_info['title']}")
            logger.info(f"🌐 URL: {page_info['url']}")
            
            # Buscar TODOS los elementos de la página
            all_data = {
                "page_info": page_info,
                "tables": [],
                "forms": [],
                "dropdowns": [],
                "buttons": [],
                "text_elements": [],
                "links": []
            }
            
            # 1. BUSCAR TODAS LAS TABLAS
            logger.info("🔍 Buscando tablas...")
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            logger.info(f"📊 Encontradas {len(tables)} tablas")
            
            for i, table in enumerate(tables):
                try:
                    table_data = self.extract_table_complete_data(table, i+1)
                    all_data["tables"].append(table_data)
                except Exception as e:
                    logger.warning(f"Error extrayendo tabla {i+1}: {str(e)}")
            
            # 2. BUSCAR TODOS LOS FORMULARIOS
            logger.info("🔍 Buscando formularios...")
            forms = self.driver.find_elements(By.TAG_NAME, "form")
            logger.info(f"📝 Encontrados {len(forms)} formularios")
            
            for i, form in enumerate(forms):
                try:
                    form_data = self.extract_form_data(form, i+1)
                    all_data["forms"].append(form_data)
                except Exception as e:
                    logger.warning(f"Error extrayendo formulario {i+1}: {str(e)}")
            
            # 3. BUSCAR TODOS LOS DROPDOWNS/SELECTS
            logger.info("🔍 Buscando dropdowns...")
            selects = self.driver.find_elements(By.TAG_NAME, "select")
            logger.info(f"📋 Encontrados {len(selects)} dropdowns")
            
            for i, select in enumerate(selects):
                try:
                    select_data = self.extract_select_data(select, i+1)
                    all_data["dropdowns"].append(select_data)
                except Exception as e:
                    logger.warning(f"Error extrayendo dropdown {i+1}: {str(e)}")
            
            # 4. BUSCAR TODOS LOS BOTONES
            logger.info("🔍 Buscando botones...")
            buttons = self.driver.find_elements(By.TAG_NAME, "button")
            inputs_buttons = self.driver.find_elements(By.CSS_SELECTOR, "input[type='button'], input[type='submit']")
            all_buttons = buttons + inputs_buttons
            logger.info(f"🔘 Encontrados {len(all_buttons)} botones")
            
            for i, button in enumerate(all_buttons):
                try:
                    button_data = self.extract_button_data(button, i+1)
                    all_data["buttons"].append(button_data)
                except Exception as e:
                    logger.warning(f"Error extrayendo botón {i+1}: {str(e)}")
            
            # 5. BUSCAR ELEMENTOS DE TEXTO IMPORTANTES
            logger.info("🔍 Buscando elementos de texto...")
            text_elements = self.driver.find_elements(By.CSS_SELECTOR, "h1, h2, h3, h4, h5, h6, p, span, div")
            logger.info(f"📝 Encontrados {len(text_elements)} elementos de texto")
            
            for i, element in enumerate(text_elements[:50]):  # Limitar a los primeros 50 para no saturar
                try:
                    if element.text.strip():
                        text_data = {
                            "index": i+1,
                            "tag": element.tag_name,
                            "text": element.text.strip(),
                            "class": element.get_attribute("class") or "",
                            "id": element.get_attribute("id") or ""
                        }
                        all_data["text_elements"].append(text_data)
                except Exception as e:
                    continue
            
            # 6. BUSCAR TODOS LOS LINKS
            logger.info("🔍 Buscando enlaces...")
            links = self.driver.find_elements(By.TAG_NAME, "a")
            logger.info(f"🔗 Encontrados {len(links)} enlaces")
            
            for i, link in enumerate(links[:20]):  # Limitar a los primeros 20
                try:
                    if link.text.strip() or link.get_attribute("href"):
                        link_data = {
                            "index": i+1,
                            "text": link.text.strip(),
                            "href": link.get_attribute("href") or "",
                            "class": link.get_attribute("class") or ""
                        }
                        all_data["links"].append(link_data)
                except Exception as e:
                    continue
            
            # Mostrar resumen de lo encontrado
            logger.info("📊 RESUMEN DE DATOS EXTRAÍDOS:")
            logger.info(f"  📊 Tablas: {len(all_data['tables'])}")
            logger.info(f"  📝 Formularios: {len(all_data['forms'])}")
            logger.info(f"  📋 Dropdowns: {len(all_data['dropdowns'])}")
            logger.info(f"  🔘 Botones: {len(all_data['buttons'])}")
            logger.info(f"  📝 Elementos de texto: {len(all_data['text_elements'])}")
            logger.info(f"  🔗 Enlaces: {len(all_data['links'])}")
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de la página: {str(e)}")
            return None
    
    def extract_table_complete_data(self, table, table_index):
        """Extrae todos los datos de una tabla específica"""
        try:
            table_data = {
                "index": table_index,
                "headers": [],
                "rows": [],
                "attributes": {
                    "class": table.get_attribute("class") or "",
                    "id": table.get_attribute("id") or ""
                }
            }
            
            # Extraer headers
            try:
                header_row = table.find_element(By.CSS_SELECTOR, "thead tr, tr:first-child")
                headers = header_row.find_elements(By.TAG_NAME, "th, td")
                table_data["headers"] = [header.text.strip() for header in headers]
            except:
                table_data["headers"] = []
            
            # Extraer todas las filas
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr, tr")
            for i, row in enumerate(rows):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td, th")
                    row_data = {
                        "row_index": i+1,
                        "cells": [cell.text.strip() for cell in cells],
                        "attributes": {
                            "class": row.get_attribute("class") or ""
                        }
                    }
                    table_data["rows"].append(row_data)
                except:
                    continue
            
            logger.info(f"📊 Tabla {table_index}: {len(table_data['headers'])} columnas, {len(table_data['rows'])} filas")
            return table_data
            
        except Exception as e:
            logger.warning(f"Error extrayendo tabla {table_index}: {str(e)}")
            return {"index": table_index, "error": str(e)}
    
    def extract_form_data(self, form, form_index):
        """Extrae datos de un formulario"""
        try:
            form_data = {
                "index": form_index,
                "action": form.get_attribute("action") or "",
                "method": form.get_attribute("method") or "",
                "inputs": [],
                "selects": [],
                "buttons": []
            }
            
            # Extraer inputs
            inputs = form.find_elements(By.TAG_NAME, "input")
            for i, input_elem in enumerate(inputs):
                input_data = {
                    "index": i+1,
                    "type": input_elem.get_attribute("type") or "",
                    "name": input_elem.get_attribute("name") or "",
                    "id": input_elem.get_attribute("id") or "",
                    "value": input_elem.get_attribute("value") or "",
                    "placeholder": input_elem.get_attribute("placeholder") or ""
                }
                form_data["inputs"].append(input_data)
            
            # Extraer selects
            selects = form.find_elements(By.TAG_NAME, "select")
            for i, select in enumerate(selects):
                select_data = {
                    "index": i+1,
                    "name": select.get_attribute("name") or "",
                    "id": select.get_attribute("id") or "",
                    "options": []
                }
                options = select.find_elements(By.TAG_NAME, "option")
                for j, option in enumerate(options):
                    option_data = {
                        "index": j+1,
                        "value": option.get_attribute("value") or "",
                        "text": option.text.strip(),
                        "selected": option.is_selected()
                    }
                    select_data["options"].append(option_data)
                form_data["selects"].append(select_data)
            
            logger.info(f"📝 Formulario {form_index}: {len(form_data['inputs'])} inputs, {len(form_data['selects'])} selects")
            return form_data
            
        except Exception as e:
            logger.warning(f"Error extrayendo formulario {form_index}: {str(e)}")
            return {"index": form_index, "error": str(e)}
    
    def extract_select_data(self, select, select_index):
        """Extrae datos de un dropdown/select"""
        try:
            select_data = {
                "index": select_index,
                "name": select.get_attribute("name") or "",
                "id": select.get_attribute("id") or "",
                "class": select.get_attribute("class") or "",
                "options": []
            }
            
            options = select.find_elements(By.TAG_NAME, "option")
            for i, option in enumerate(options):
                option_data = {
                    "index": i+1,
                    "value": option.get_attribute("value") or "",
                    "text": option.text.strip(),
                    "selected": option.is_selected()
                }
                select_data["options"].append(option_data)
            
            logger.info(f"📋 Dropdown {select_index}: {len(select_data['options'])} opciones")
            return select_data
            
        except Exception as e:
            logger.warning(f"Error extrayendo dropdown {select_index}: {str(e)}")
            return {"index": select_index, "error": str(e)}
    
    def extract_button_data(self, button, button_index):
        """Extrae datos de un botón"""
        try:
            button_data = {
                "index": button_index,
                "text": button.text.strip(),
                "type": button.get_attribute("type") or "",
                "class": button.get_attribute("class") or "",
                "id": button.get_attribute("id") or "",
                "onclick": button.get_attribute("onclick") or ""
            }
            
            logger.info(f"🔘 Botón {button_index}: '{button_data['text']}'")
            return button_data
            
        except Exception as e:
            logger.warning(f"Error extrayendo botón {button_index}: {str(e)}")
            return {"index": button_index, "error": str(e)}
    
    def select_event_and_extract_data(self):
        """Selecciona un evento del dropdown y extrae los datos de las tablas"""
        try:
            logger.info("Iniciando selección de evento y extracción de datos...")
            
            # Esperar a que la página esté completamente cargada
            time.sleep(3)
            
            # Buscar el dropdown de eventos
            event_dropdown = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "eventSelect"))
            )
            
            logger.info("Dropdown de eventos encontrado, haciendo clic para desplegar...")
            event_dropdown.click()
            
            # Esperar a que se despliegue la lista
            time.sleep(2)
            
            # Buscar todas las opciones del dropdown
            try:
                # Intentar encontrar las opciones en el dropdown desplegado
                dropdown_options = self.driver.find_elements(By.CSS_SELECTOR, ".e-dropdownlist .e-list-item, .e-autocomplete .e-list-item, [role='option']")
                
                if not dropdown_options:
                    # Intentar con otros selectores
                    dropdown_options = self.driver.find_elements(By.CSS_SELECTOR, ".e-popup .e-list-item, .e-dropdown .e-list-item")
                
                if dropdown_options:
                    logger.info(f"Encontradas {len(dropdown_options)} opciones en el dropdown:")
                    
                    # Mostrar las opciones encontradas
                    for i, option in enumerate(dropdown_options[:5]):  # Mostrar solo las primeras 5
                        try:
                            option_text = option.text.strip()
                            if option_text:
                                logger.info(f"  {i+1}. {option_text}")
                        except:
                            continue
                    
                    # Seleccionar el primer evento (si hay más de uno)
                    if len(dropdown_options) > 1:
                        logger.info("Seleccionando el primer evento de la lista...")
                        dropdown_options[0].click()
                        time.sleep(2)
                    else:
                        logger.info("Solo hay un evento disponible, ya está seleccionado")
                        
                else:
                    logger.warning("No se encontraron opciones en el dropdown")
                    
            except Exception as e:
                logger.warning(f"Error buscando opciones del dropdown: {str(e)}")
            
            # Ahora extraer los datos de las tablas
            return self.extract_table_data()
            
        except Exception as e:
            logger.error(f"Error seleccionando evento: {str(e)}")
            return False
    
    def detect_table_type(self, headers, first_row_data):
        """
        Detecta el tipo de tabla basado en los headers y datos de la primera fila
        
        Returns:
            str: 'evento' para tablas con eventos específicos, 'categoria' para tablas de categorías
        """
        try:
            # Verificar si la primera columna es "Evento" (tabla de eventos)
            if len(headers) > 0 and "evento" in headers[0].lower():
                return "evento"
            
            # Verificar si la primera columna es "Categoria" (tabla de categorías)
            if len(headers) > 0 and "categoria" in headers[0].lower():
                return "categoria"
            
            # Verificar por el contenido de la primera fila
            if first_row_data and len(first_row_data) > 0:
                first_cell = first_row_data[0].strip().lower()
                if "evento" in first_cell or "total evento" in first_cell:
                    return "evento"
                elif "capacidad" in first_cell or "valor entrada" in first_cell:
                    return "categoria"
            
            # Por defecto, asumir que es tabla de eventos
            return "evento"
            
        except Exception as e:
            logger.warning(f"Error detectando tipo de tabla: {str(e)}")
            return "evento"

    def extract_event_info_from_dropdown(self):
        """
        Extrae información del evento desde el dropdown cuando está disponible
        """
        try:
            # Buscar el dropdown de eventos
            event_dropdown = self.driver.find_element(By.ID, "eventSelect")
            event_value = event_dropdown.get_attribute("value")
            
            if event_value and event_value.strip():
                logger.info(f"🎭 Evento encontrado en dropdown: {event_value}")
                return event_value.strip()
            
            return None
            
        except Exception as e:
            logger.warning(f"No se pudo extraer evento del dropdown: {str(e)}")
            return None

    def extract_date_from_dropdown(self):
        """
        Extrae la fecha del evento desde el dropdown de función
        """
        try:
            # Buscar el dropdown de función
            function_dropdown = self.driver.find_element(By.ID, "calendarioId")
            
            # Obtener el texto del primer option disponible (ya que puede no tener selected)
            options = function_dropdown.find_elements(By.TAG_NAME, "option")
            if options:
                function_text = options[0].text.strip()
                
                if function_text and function_text.strip():
                    # Extraer fecha del formato "2025-11-30 21:00 - Domingo"
                    if " - " in function_text:
                        date_part = function_text.split(" - ")[0]  # "2025-11-30 21:00"
                        if date_part:
                            # Convertir a formato datetime
                            from datetime import datetime
                            fecha_obj = datetime.strptime(date_part, "%Y-%m-%d %H:%M")
                            logger.info(f"📅 Fecha extraída del dropdown: {fecha_obj}")
                            return fecha_obj.strftime("%d/%m/%Y %H:%M")
            
            return None
            
        except Exception as e:
            logger.warning(f"No se pudo extraer fecha del dropdown: {str(e)}")
            return None

    def extract_date_from_event_data(self, data_row):
        """
        Extrae la fecha del evento desde los datos de la fila
        """
        try:
            # Buscar la columna de fecha en los datos
            for key, value in data_row.items():
                if "fecha" in key.lower() and value and value != "Total":
                    return value
            return None
            
        except Exception as e:
            logger.warning(f"Error extrayendo fecha de datos: {str(e)}")
            return None

    def extract_table_data(self):
        """Extrae los datos completos de las tablas de ventas con todos los campos"""
        try:
            logger.info("Extrayendo datos completos de las tablas...")
            
            # Esperar a que la página esté completamente cargada
            time.sleep(2)
            
            # Buscar el contenedor de datos
            data_container = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "dataContainer"))
            )
            
            # Buscar los títulos h4 y las tablas
            h4_elements = data_container.find_elements(By.TAG_NAME, "h4")
            tables = data_container.find_elements(By.TAG_NAME, "table")
            
            logger.info(f"Encontrados {len(h4_elements)} títulos y {len(tables)} tablas")
            
            extracted_data = []
            table_type = None
            
            for table_index, table in enumerate(tables):
                try:
                    # Obtener el título de la tabla del h4 correspondiente
                    table_title = ""
                    if table_index < len(h4_elements):
                        table_title = h4_elements[table_index].text.strip()
                    else:
                        table_title = f"Tabla {table_index + 1}"
                    
                    logger.info(f"Procesando tabla: {table_title}")
                    
                    # Extraer los headers de la tabla
                    headers = []
                    try:
                        header_row = table.find_element(By.CSS_SELECTOR, "thead tr")
                        header_cells = header_row.find_elements(By.TAG_NAME, "th")
                        headers = [cell.text.strip() for cell in header_cells]
                        logger.info(f"Headers encontrados ({len(headers)}): {headers[:5]}...")  # Mostrar solo los primeros 5
                    except Exception as e:
                        logger.warning(f"Error extrayendo headers: {str(e)}")
                        continue
                    
                    # Buscar las filas de datos (incluir todas las filas del tbody)
                    data_rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                    logger.info(f"Encontradas {len(data_rows)} filas de datos")
                    
                    # Detectar el tipo de tabla en la primera iteración
                    if table_index == 0 and data_rows:
                        first_row_cells = data_rows[0].find_elements(By.TAG_NAME, "td")
                        first_row_data = [cell.text.strip() for cell in first_row_cells]
                        table_type = self.detect_table_type(headers, first_row_data)
                        logger.info(f"🔍 Tipo de tabla detectado: {table_type}")
                    
                    for row_index, row in enumerate(data_rows):
                        try:
                            # Verificar si es una fila de total
                            is_total_row = "total" in row.get_attribute("class")
                            is_bold_row = "b" in row.get_attribute("class")
                            
                            # Extraer las celdas
                            cells = row.find_elements(By.TAG_NAME, "td")
                            
                            if len(cells) > 0:
                                # Crear un diccionario con todos los datos de la fila
                                data_row = {
                                    "tabla": table_title,
                                    "tabla_tipo": "cantidad" if "cantidad" in table_title.lower() else "monto",
                                    "es_total": is_total_row,
                                    "es_encabezado": is_bold_row,
                                    "fila_numero": row_index + 1
                                }
                                
                                # Mapear cada celda con su header correspondiente
                                for i, cell in enumerate(cells):
                                    header_name = headers[i] if i < len(headers) else f"Columna_{i+1}"
                                    cell_text = cell.text.strip()
                                    
                                    # Normalizar el nombre del header para usar como clave
                                    header_key = header_name.lower().replace(" ", "_").replace("ñ", "n").replace("  ", "_")
                                    
                                    # Limpiar caracteres especiales del header_key
                                    header_key = re.sub(r'[^\w]', '_', header_key)
                                    header_key = re.sub(r'_+', '_', header_key).strip('_')
                                    
                                    data_row[header_key] = cell_text if cell_text else "0"
                                
                                # Para tablas de eventos, extraer información del evento de la primera celda
                                if table_type == "evento" and len(cells) > 0:
                                    first_cell_text = cells[0].text.strip()
                                    if first_cell_text and first_cell_text != "Total Evento":
                                        data_row["evento"] = first_cell_text
                                        data_row["fecha_evento"] = self.extract_date_from_event_data(data_row)
                                
                                extracted_data.append(data_row)
                                
                                # Log de la información extraída (solo para filas importantes)
                                if row_index < 3 or is_total_row:
                                    categoria = data_row.get('categoria', data_row.get('evento', 'N/A'))
                                    total = data_row.get('total', 'N/A')
                                    logger.info(f"  - Fila {row_index + 1}: {categoria} | Total: {total}")
                                
                        except Exception as e:
                            logger.warning(f"Error procesando fila {row_index}: {str(e)}")
                            continue
                            
                except Exception as e:
                    logger.warning(f"Error procesando tabla {table_index}: {str(e)}")
                    continue
            
            # Si es tabla de categorías, intentar extraer información del evento del dropdown
            if table_type == "categoria":
                event_info = self.extract_event_info_from_dropdown()
                if event_info:
                    # Agregar información del evento a todos los registros
                    for data_row in extracted_data:
                        data_row["evento"] = event_info
                        data_row["fecha_evento"] = self.extract_date_from_dropdown()
            
            # Guardar los datos extraídos en la base de datos
            if extracted_data:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                current_user = self.get_current_credential_info()
                
                # Agregar metadata sobre la extracción
                final_data = {
                    "metadata": {
                        "usuario_utilizado": current_user,
                        "timestamp": timestamp,
                        "fecha_extraccion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "total_registros": len(extracted_data),
                        "tipo_tabla": table_type
                    },
                    "datos": extracted_data
                }
                
                # Guardar en la base de datos
                if self.save_to_database(final_data):
                    logger.info(f"✅ Datos extraídos y guardados en base de datos")
                    logger.info(f"Usuario utilizado: {current_user}")
                    logger.info(f"Tipo de tabla: {table_type}")
                    logger.info(f"Total de registros extraídos: {len(extracted_data)}")
                    return True
                else:
                    logger.error("❌ Error guardando datos en base de datos")
                    return False
            else:
                logger.warning("No se encontraron datos para extraer")
                return False
                
        except Exception as e:
            logger.error(f"Error extrayendo datos de las tablas: {str(e)}")
            return False
    

    
    def run_with_credential(self, credential_index):
        """Ejecuta el scraper completo con una credencial específica"""
        try:
            credential = self.credentials[credential_index]
            username = credential["username"]
            
            logger.info(f"=== INICIANDO PROCESO CON CREDENCIAL {credential_index + 1}/{len(self.credentials)}: {username} ===")
            
            # Navegar a la página de login
            if not self.navigate_to_login():
                logger.error(f"No se pudo navegar a la página de login para {username}")
                return False
            
            # Realizar login con credencial específica
            if not self.login_with_specific_credential(credential_index):
                logger.error(f"No se pudo realizar el login con {username}")
                return False
            
            # Obtener información de la página inicial
            self.get_page_info()
            
            # Navegar directamente a Evolución Ventas
            if self.navigate_to_evolucion_ventas():
                # Obtener información de la página de Evolución Ventas
                self.get_evolucion_ventas_info()
                
                # Extraer datos específicos del evento y fecha
                event_data = self.extract_event_and_date_data()
                
                if event_data:
                    logger.info("✅ Extracción de datos del evento exitosa")
                    logger.info("🧪 MODO TEST - Datos extraídos del evento:")
                    logger.info(f"  🎭 Evento: {event_data['evento_nombre']}")
                    logger.info(f"  📅 Fecha: {event_data['evento_fecha']}")
                    logger.info(f"  🆔 ID: {event_data['evento_id']}")
                    logger.info(f"  🎤 Artista: {event_data['artista']}")
                    logger.info(f"  🏟️ Venue: {event_data['venue']}")
                    
                    # Hacer matching con la base de datos
                    matching_results = self.match_with_database(event_data)
                    
                    if matching_results:
                        logger.info("🎯 MATCHING EXITOSO CON LA BASE DE DATOS")
                        
                        # VALIDAR datos antes de guardar (MODO VALIDACIÓN)
                        show_id = matching_results[0][0]  # Usar el primer resultado del matching (ID está en posición 0)
                        
                        logger.info("🔍 VALIDANDO DATOS ANTES DE GUARDAR...")
                        validation_result = self.validate_daily_sales_data(event_data, show_id)
                        
                        if validation_result:
                            logger.info("✅ VALIDACIÓN COMPLETADA - Datos listos para guardar")
                            logger.info("💡 Para activar el guardado, cambiar a modo producción")
                        else:
                            logger.error("❌ ERROR EN VALIDACIÓN DE DATOS")
                    else:
                        logger.warning("⚠️ NO SE ENCONTRÓ MATCHING EN LA BASE DE DATOS")
                else:
                    logger.error("❌ No se pudieron extraer los datos del evento")
            else:
                logger.error("❌ No se pudo navegar a Evolución Ventas")
            
            logger.info(f"=== PROCESO COMPLETADO EXITOSAMENTE CON {username} ===")
            return True
            
        except Exception as e:
            logger.error(f"Error ejecutando scraper con {username}: {str(e)}")
            return False

    def run(self):
        """Ejecuta el scraper completo con todas las credenciales para Airflow"""
        try:
            logger.info("=== INICIANDO SCRAPER DE TELETICKET PARA AIRFLOW ===")
            logger.info("🧪 MODO VALIDACIÓN ACTIVADO - Los datos se validarán SIN guardar en la base de datos")
            
            # Configurar driver
            if not self.setup_driver():
                logger.error("No se pudo configurar el driver")
                return self.final_data
            
            successful_runs = 0
            failed_runs = 0
            
            # Ejecutar el proceso completo con cada credencial
            for i in range(len(self.credentials)):
                try:
                    if self.run_with_credential(i):
                        successful_runs += 1
                        logger.info(f"Credencial {i+1} procesada exitosamente")
                    else:
                        failed_runs += 1
                        logger.warning(f"Credencial {i+1} falló")
                    
                    # Pequeña pausa entre credenciales
                    if i < len(self.credentials) - 1:  # No hacer pausa después de la última
                        logger.info("Esperando antes de procesar siguiente credencial...")
                        time.sleep(2)
                        
                except Exception as e:
                    failed_runs += 1
                    logger.error(f"Error procesando credencial {i+1}: {str(e)}")
                    continue
            
            # Actualizar datos finales para Airflow
            self.final_data.update({
                "fecha_extraccion": (datetime.now() - timedelta(hours=3)).isoformat(),
                "total_eventos_procesados": len(self.credentials),
                "eventos_exitosos": successful_runs,
                "eventos_con_error": failed_runs
            })
            
            # Resumen final
            logger.info(f"=== RESUMEN FINAL ===")
            logger.info(f"Credenciales procesadas exitosamente: {successful_runs}")
            logger.info(f"Credenciales que fallaron: {failed_runs}")
            logger.info(f"Total credenciales: {len(self.credentials)}")
            logger.info(f"✅ Datos guardados únicamente en base de datos")
            
            return self.final_data
            
        except Exception as e:
            logger.error(f"Error ejecutando scraper: {str(e)}")
            return self.final_data
        
        finally:
            # Cerrar navegador automáticamente
            if self.driver:
                logger.info("Cerrando navegador automáticamente...")
                self.driver.quit()
                logger.info("Driver cerrado")
    
    def close(self):
        """Cierra el driver"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")

def main():
    """Función principal para ejecución local"""
    scraper = TeleticketScraper(headless=True)
    try:
        result = scraper.run()
        print("✅ Scraper ejecutado exitosamente")
        print(f"📊 Resultados: {result}")
        return result
    except KeyboardInterrupt:
        logger.info("Scraper interrumpido por el usuario")
        return None
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return None
    finally:
        scraper.close()

def run_scraper_for_airflow():
    """Función para ejecutar el scraper desde Airflow"""
    try:
        scraper = TeleticketScraper(headless=True)
        result = scraper.run()
        scraper.close()
        return result
    except Exception as e:
        logger.error(f"Error ejecutando scraper para Airflow: {str(e)}")
        return {
            "ticketera": "Teleticket",
            "fecha_extraccion": "",
            "total_eventos_procesados": 0,
            "eventos_exitosos": 0,
            "eventos_con_error": 1,
            "datos_por_evento": {},
            "error": str(e)
        }

def test_local():
    """Función para testing local con navegador visible"""
    scraper = TeleticketScraper(headless=False)
    try:
        result = scraper.run()
        print("✅ Scraper ejecutado exitosamente")
        print(f"📊 Resultados: {result}")
        return result
    except KeyboardInterrupt:
        logger.info("Scraper interrumpido por el usuario")
        return None
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return None
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
