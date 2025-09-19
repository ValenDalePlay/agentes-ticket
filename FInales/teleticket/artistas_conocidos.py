# Lista de artistas conocidos para Teleticket
# Esta lista se usa para validar y corregir nombres de artistas extraídos

ARTISTAS_CONOCIDOS = [
    # Artistas principales
    "MILO J",
    "ERREWAY", 
    "CAZZU",
    
    # Otros artistas que podrían aparecer
    "DIEGO TORRES",
    "AIRBAG",
    "TINI",
    "LALI",
    "EMANUEL NOIR",
    "DANTE SPINETTA",
    "NICKI NICOLE",
    "BENEDETTA",
    "MARIA BECERRA",
    "LIT KILLAH",
    "DUKI",
    "TRUENO",
    "BHAVI",
    "KHEA",
    "RUSHERKING",
    "FMK",
    "BANDALOS CHINOS",
    "LAS PASTILLAS DEL ABUELO",
    "DIVIDIDOS",
    "LOS AUTENTICOS DECADENTES",
    "BABASONICOS",
    "MIRANDA!",
    "EMANUEL HORVILLEUR",
    "FITO PAEZ",
    "CHAYANNE",
    "RICARDO MONTANER",
    "ALEJANDRO SANZ",
    "ENRIQUE IGLESIAS",
    "MALUMA",
    "J BALVIN",
    "BAD BUNNY",
    "OZUNA",
    "KAROL G",
    "ANITTA",
    "ROSALIA",
    "CAMILA CABELLO",
    "SHAKIRA",
    "MALUMA",
    "SEBASTIAN YATRA",
    "CARLOS VIVES",
    "JUANES",
    "MANA",
    "CAFE TACVBA",
    "MOLOTOV",
    "PLASTILINA MOSH",
    "JULIETA VENEGAS",
    "NATALIA LAFOURCADE",
    "MON LAFERTE",
    "GEORGINA",
    "LOLA INDIGO",
    "ROSALIA",
    "C TANGANA",
    "AYER Y HOY",
    "LOS RODRIGUEZ",
    "SODA STEREO",
    "GUSTAVO CERATI",
    "CHICOS DE BARRIO",
    "LOS PERICOS",
    "ILYA KURYAKI",
    "BABASONICOS",
    "MIRANDA!",
    "EMANUEL HORVILLEUR",
    "FITO PAEZ",
    "CHAYANNE",
    "RICARDO MONTANER",
    "ALEJANDRO SANZ",
    "ENRIQUE IGLESIAS",
    "MALUMA",
    "J BALVIN",
    "BAD BUNNY",
    "OZUNA",
    "KAROL G",
    "ANITTA",
    "ROSALIA",
    "CAMILA CABELLO",
    "SHAKIRA",
    "MALUMA",
    "SEBASTIAN YATRA",
    "CARLOS VIVES",
    "JUANES",
    "MANA",
    "CAFE TACVBA",
    "MOLOTOV",
    "PLASTILINA MOSH",
    "JULIETA VENEGAS",
    "NATALIA LAFOURCADE",
    "MON LAFERTE",
    "GEORGINA",
    "LOLA INDIGO",
    "ROSALIA",
    "C TANGANA",
    "AYER Y HOY",
    "LOS RODRIGUEZ",
    "SODA STEREO",
    "GUSTAVO CERATI",
    "CHICOS DE BARRIO",
    "LOS PERICOS",
    "ILYA KURYAKI"
]

def validar_artista(artista_extraido):
    """
    Valida y corrige el nombre del artista usando la lista de artistas conocidos
    
    Args:
        artista_extraido (str): Nombre del artista extraído del scraper
        
    Returns:
        str: Nombre del artista validado y corregido
    """
    if not artista_extraido or artista_extraido.strip() == "":
        return ""
    
    artista_limpio = artista_extraido.strip().upper()
    
    # Buscar coincidencia exacta
    for artista_conocido in ARTISTAS_CONOCIDOS:
        if artista_conocido.upper() == artista_limpio:
            return artista_conocido
    
    # Buscar coincidencia parcial (el artista conocido contiene el extraído)
    for artista_conocido in ARTISTAS_CONOCIDOS:
        if artista_limpio in artista_conocido.upper():
            return artista_conocido
    
    # Buscar coincidencia parcial (el extraído contiene el artista conocido)
    for artista_conocido in ARTISTAS_CONOCIDOS:
        if artista_conocido.upper() in artista_limpio:
            return artista_conocido
    
    # Si no se encuentra coincidencia, devolver el original
    return artista_extraido.strip()

def agregar_artista_nuevo(nombre_artista):
    """
    Agrega un nuevo artista a la lista de artistas conocidos
    
    Args:
        nombre_artista (str): Nombre del nuevo artista
    """
    if nombre_artista and nombre_artista.strip() not in ARTISTAS_CONOCIDOS:
        ARTISTAS_CONOCIDOS.append(nombre_artista.strip())
        print(f"✅ Artista agregado: {nombre_artista.strip()}")

def obtener_artistas_conocidos():
    """
    Retorna la lista de artistas conocidos
    
    Returns:
        list: Lista de artistas conocidos
    """
    return ARTISTAS_CONOCIDOS.copy()

if __name__ == "__main__":
    # Pruebas
    print("🎤 Artistas conocidos:")
    for artista in ARTISTAS_CONOCIDOS[:10]:  # Mostrar solo los primeros 10
        print(f"  - {artista}")
    
    print(f"\n📊 Total de artistas conocidos: {len(ARTISTAS_CONOCIDOS)}")
    
    # Pruebas de validación
    print("\n🧪 Pruebas de validación:")
    test_cases = ["CAZZU", "MILO J", "ERREWAY", "AREQUIPA", "VEN057 - CAZZU - LATINAJE"]
    for test in test_cases:
        resultado = validar_artista(test)
        print(f"  '{test}' -> '{resultado}'")
