# 📊 ETL de Datos Históricos de la Bolsa de Valores de Colombia (BVC)

## 🚀 Descripción
Este proyecto implementa un flujo **ETL (Extracción, Transformación y Carga)** de datos históricos de la **Bolsa de Valores de Colombia (BVC)**.  
El objetivo es obtener, limpiar y almacenar datos financieros de alta calidad en una base de datos **MySQL**, gestionada en un entorno local con **XAMPP**.  

Con este proyecto se busca crear una base sólida para futuros análisis financieros, modelado de series temporales y visualización de indicadores de mercado.  

---

## 🎯 Objetivos del Proyecto
- Extraer datos históricos de la BVC desde Yahoo Finance.  
- Limpiar, transformar y normalizar la información para asegurar consistencia.  
- Almacenar los datos en una base de datos MySQL local.  
- Preparar el dataset para futuros análisis financieros y dashboards de inversión.  

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
- Integrar visualizaciones en Power BI.  

---

## 👤 Autor
**Camilo García Rey**  
- LinkedIn: https://www.linkedin.com/in/camilo-garcia-rey/  
- GitHub Portfolio: github.com/camilogrey 
