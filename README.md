# 📊 ETL de Datos Históricos de la BVC y del IBEX 35 

## 🚀 Descripción
Este proyecto implementa un flujo **ETL (Extracción, Transformación y Carga)** de datos históricos de la **Bolsa de Valores de Colombia (BVC) y el índice bursátil de referencia en el mercado bursátil español (IBEX 35)**.  
El objetivo es obtener, limpiar y almacenar datos financieros disponibles y de calidad en una base de datos **MySQL**, gestionada en un entorno local con **XAMPP**.  

Con este proyecto se busca crear una base sólida para futuros análisis financieros, modelado de series temporales y visualización de indicadores de mercado; y futuras versiones integrando mas nemotecnicos.  

---

## 🎯 Objetivos del Proyecto
- Extraer datos históricos de la BVC y el IBEX 35 desde Yahoo Finance.  
- Limpiar, transformar y normalizar la información para asegurar consistencia.  
- Almacenar los datos en una base de datos MySQL local.  
- Preparar el dataset para futuros análisis y comparaciones generales financieros y dashboards Sectoriales y de Acciones.  

---

## 🗂️ Dataset
- **Fuente:** [Yahoo Finance](https://finance.yahoo.com/)  
- **Variables clave:** precio de apertura, cierre, máximo, mínimo, volumen transado.  
- **Alcance temporal:** datos históricos de la BVC (acciones listadas).  
- **Procesamiento:**  
  - Web scraping con Python.  
  - Limpieza de duplicados, formateo de fechas y estandarización de columnas.  

---

## 🛠️ Tecnologías y Herramientas
- **Lenguaje:** Python 3.x  
- **Librerías:** pandas, SQLAlchemy, PyMySQL  
- **Base de datos:** MySQL (XAMPP en servidor local)  
- **Entorno:** XAMPP para gestión de base de datos y pruebas locales  

---

## 📈 Metodología
1. **Extracción:** Web scraping desde Yahoo Finance.  
2. **Transformación:**  
   - Normalización de nombres de columnas.  
   - Conversión de formatos de fecha y moneda.  
   - Depuración de datos inconsistentes.  
3. **Carga:** Inserción en tablas MySQL mediante SQLAlchemy.  
4. **Almacenamiento:** Base de datos gestionada en servidor local con XAMPP.
5. Transformación de datos y ajustes de modelado de datos para la presentacion de reportes.   

---

## 📊 Resultados
- Base de datos local con información histórica consolidada.  
- Datos listos para análisis de series temporales, dashboards financieros y backtesting de estrategias.  

---

## 💡 Posibles Usos
- Construcción de modelos predictivos de precios de acciones.  
- Análisis de volatilidad y correlación entre activos.  
- Dashboards de mercado en Power BI o Tableau.  

---

## 📌 Próximos pasos
- Implementar pipelines en la nube (AWS RDS / GCP BigQuery).  
- Automatizar la actualización diaria de datos.
- Aumentar el numero de mercados e indices 
- Integrar visualizaciones en Power BI.
- Analisis de series de tiempo y aplicación de algoritmos de Machine Learcni

---

## 👤 Autor
**Camilo García Rey**  
- LinkedIn: https://www.linkedin.com/in/camilo-garcia-rey/  
- GitHub Portfolio: github.com/camilogrey 
