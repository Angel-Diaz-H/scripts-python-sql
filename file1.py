import os
import re
from collections import defaultdict

archivo_entrada = "/content/entrada.sql"

#Validar que exista el archivo antes de continuar.
if not os.path.exists(archivo_entrada):
    print("Archivo no encontrado. Cargar el archivo.")
    input("Pulsa ENTER para cerrar.")
    exit() #No efectua cambios en Google Collab.

#1. Generar nombre de archivo backup.
print("🔵 Nombre del backup. 🔵")
#print("Backup_[Tipo]_[Número]_[TareaYNúmero]_Parte[Número]_[Descripción].[Extensión]\n")

#Tipo.
print("🌐 Ingresa el tipo:")
print("1. Ticket.")
print("2. RFC.")
while True:
    tipo_opcion = input("▶ Opción: ").strip()
    if tipo_opcion in ("1", "2"):
        tipo = "Ticket" if tipo_opcion == "1" else "RFC"
        break
    else:
        print("⚠️ Opción inválida. Ingresa 1 o 2.")

#Número.
print(f"\n🌐 Ingresa el número de {tipo}:")
numero = input(f"▶ Número de {tipo}: ").strip()
while not numero:
    print("⚠️ Obligatorio.")
    numero = input(f"▶ Número de {tipo}: ").strip()

#Tarea.
print("\n🌐 Ingresa la tarea:")
print("1. Inicial1.")
print("2. Complementaria.")
while True:
    tarea_opcion = input("▶ Opción: ").strip()
    if tarea_opcion == "1":
        tarea = "Inicial1"
        break
    elif tarea_opcion == "2":
        tarea_num = input("▶ Número de complementaria: ").strip()
        while not tarea_num.isdigit():
            print("⚠️ Debe ser un número.")
            tarea_num = input("▶ Número de complementaria: ").strip()
        tarea = f"Complementaria{tarea_num}"
        break
    else:
        print("⚠️ Opción inválida. Ingresa 1 o 2.")

#Parte.
print(f"\n🌐 Ingresa la parte de la {tarea}:")
parte = input(f"▶ Parte: ").strip()
while not parte.isdigit():
    print("⚠️ Debe ser un número.")
    parte = input(f"▶ Parte: ").strip()

#Descripción.
print("\n🌐 Agregar una descripción:")
descripcion = input("▶ Descripción (opcional): ").strip()

#Extensión.
print("\n🌐 ¿El archivo tendrá extensión .sql?")
print("1. Sí")
print("2. No")
while True:
    extension_opcion = input("▶ Opción: ").strip()
    if extension_opcion == "1":
        extension = "sql"
        break
    elif extension_opcion == "2":
        extension = input("▶ Ingresa la extensión deseada (ejemplo txt, csv): ").strip().lower()
        while not extension:
            print("⚠️ Obligatorio.")
            extension = input("▶ Extensión deseada: ").strip().lower()
        break
    else:
        print("⚠️ Opción inválida. Ingresa 1 o 2.")

# Generar nombre final
nombre_archivo = f"Backup_{tipo}_{numero}_{tarea}_Parte{parte}"
if descripcion:
    nombre_archivo += f"_{descripcion}"
nombre_archivo += f".{extension}"

#print(f"\nNombre de archivo final generado: {nombre_archivo}\n")

# ---------------------------------------------------------------------------------

#2. Procesar archivo SQL de entrada y agrupar.
archivo_entrada = "/content/entrada.sql"
archivo_salida = nombre_archivo

with open(archivo_entrada, "r") as f:
    contenido = f.read()

sentencias = contenido.splitlines()
agrupaciones = {}

for sentencia in sentencias:
    sentencia = sentencia.strip()
    if not sentencia or sentencia.startswith("--"):
        continue

    sentencia_upper = sentencia.upper()
    tabla = None
    if sentencia_upper.startswith("DELETE FROM"):
        match_tabla = re.match(r"DELETE FROM\s+([^\s]+)", sentencia, re.IGNORECASE)
    elif sentencia_upper.startswith("UPDATE"):
        match_tabla = re.match(r"UPDATE\s+([^\s]+)", sentencia, re.IGNORECASE)
    else:
        continue

    if not match_tabla:
        continue

    tabla = match_tabla.group(1)
    where_idx = sentencia_upper.find("WHERE")
    if where_idx == -1:
        continue

    condiciones_raw = sentencia[where_idx + len("WHERE"):].strip()
    condiciones = [c.strip() for c in re.split(r"\s+AND\s+", condiciones_raw, flags=re.IGNORECASE)]
    columnas = []
    valores_por_columna = defaultdict(set)

    for cond in condiciones:
        match_eq = re.match(r"(\w+)\s*=\s*(.+)", cond)
        if match_eq:
            columna = match_eq.group(1).strip()
            valor = match_eq.group(2).strip()
            columnas.append(columna)
            valores_por_columna[columna].add(valor)
            continue

        match_in = re.match(r"(\w+)\s+IN\s*\((.+)\)", cond, re.IGNORECASE)
        if match_in:
            columna = match_in.group(1).strip()
            valores_raw = match_in.group(2).strip()
            valores = [v.strip() for v in valores_raw.split(",")]
            columnas.append(columna)
            for valor in valores:
                valores_por_columna[columna].add(valor)
            continue

    columnas_tuple = tuple(sorted(columnas))
    clave = (tabla, columnas_tuple)
    if clave not in agrupaciones:
        agrupaciones[clave] = defaultdict(set)
    for columna, valores in valores_por_columna.items():
        agrupaciones[clave][columna].update(valores)

# ---------------------------------------------------------------------------------

#3. Generar y guardar archivo final.
with open(archivo_salida, "w") as f:
    for (tabla, columnas), valores_por_columna in agrupaciones.items():
        condiciones = []
        for columna in columnas:
            valores = valores_por_columna[columna]
            valores_formateados = ", ".join(valores)
            condiciones.append(f"{columna} IN ({valores_formateados})")
        where_final = " AND ".join(condiciones)
        select = f"SELECT * FROM {tabla} WHERE {where_final};"
        f.write(select + "\n")

print(f"\n\n✅ Archivo '{archivo_salida}' generado exitosamente con {len(agrupaciones)} SELECTs.")
