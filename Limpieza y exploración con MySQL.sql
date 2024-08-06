-- Limpieza de datos

-- 1: Eliminar duplicados
-- 2: Estandarizar los datos
-- 3: Valores nulos o en blanco
-- 4: Eliminar columnas innecesarias

-- Cambiamos el nombre a "layoffs_raw" para que la tabla original quede intacta
ALTER TABLE layoffs
RENAME TO layoffs_raw;

-- Creamos una tabla idéntica para limpiar los datos
CREATE TABLE layoffs_working
LIKE layoffs_raw;

-- Y añadimos los datos
INSERT INTO layoffs_working
SELECT *
FROM layoffs_raw;

-- 1: Eliminar duplicados
-- Nuestra tabla no tiene una primary key con valores únicos, por lo que utilizaremos una combinación de todos los campos para determinar si un registro está duplicado
WITH duplicate_CTE AS 
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_working
)
SELECT * 
FROM duplicate_CTE
WHERE row_num > 1;

-- Vamos a crear una tabla adicional donde podamos agregar la columna "row_number" y así eliminar los duplicados
CREATE TABLE `layoffs_working2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_working2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_working;

DELETE
FROM layoffs_working2
WHERE row_num > 1;

-- 2: Estandarizar los datos

SELECT company, TRIM(company)
FROM layoffs_working2;

-- Existen datos en la columna "company" con espacios al principio. Vamos a eliminarlos:

UPDATE layoffs_working2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_working2;

-- En el campo "industry", hay tres términos que hacen referencia a lo mismo: "Crypto", "CryptoCurrency" y "Crypto Currency". Vamos a actualizarlos a "Crypto".

UPDATE layoffs_working2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

SELECT DISTINCT(country)
FROM layoffs_working2
ORDER BY 1;

-- Algunos registros tienen el país "United States" con un punto final que debemos eliminar.

UPDATE layoffs_working2
SET country = TRIM(TRAILING "." FROM country)
WHERE country LIKE "United States%";

-- La columna "date" tiene formato "mm/dd/aaaa" y su tipo de dato es "Text". 
-- Primero cambiamos el formato:
UPDATE layoffs_working2
SET `date` = STR_TO_DATE(`date`, "%m/%d/%Y");

-- Y luego modificamos el tipo de dato del campo:
ALTER TABLE layoffs_working2
MODIFY COLUMN `date` DATE;

-- 3: Valores nulos o en blanco
SELECT *
FROM layoffs_working2
WHERE industry IS NULL
OR industry = "";

-- Tenemos 4 empresas con valores nulos, 3 de las cuales tienen otros registros donde la industria sí está especificada. Utilizaremos estos registros para llenar los valores nulos.

SELECT t1.company, t1.industry, t2.industry
FROM layoffs_working2 t1
JOIN layoffs_working2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = "")
AND (t2.industry IS NOT NULL AND t2.industry != "");

UPDATE layoffs_working2 t1
JOIN layoffs_working2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = "")
AND (t2.industry IS NOT NULL AND t2.industry != "");

-- 4: Eliminar columnas o registros innecesarios

SELECT *
FROM layoffs_working2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Como vamos a investigar sobre el número y porcentaje de despidos, los registros donde ambas columnas sean NULL no nos servirán. Eliminémoslos:

DELETE
FROM layoffs_working2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Eliminamos la columna que creamos al principio para los valores duplicados
ALTER TABLE layoffs_working2
DROP COLUMN row_num;

-- Por último, vamos a eliminar la tabla intermedia y cambiar el nombre de la tabla final que utilizaremos para nuestro análisis:

DROP TABLE layoffs_working;

ALTER TABLE layoffs_working2
RENAME TO layoffs_analysis;

-- Exploración de los datos

-- Rango de fechas de nuestros datos
SELECT MIN(date), MAX(date)
FROM layoffs_analysis;

-- Total de despidos por empresa
SELECT 
	company, 
    SUM(total_laid_off) AS total_off
FROM layoffs_analysis
GROUP BY company
ORDER BY 2 DESC;

-- ¿Cuál fue el máximo de empleados despedidos por una empresa en un solo día? ¿Qué empresa fue?
SELECT 
	company,
    total_laid_off
FROM layoffs_analysis
WHERE total_laid_off = (
					SELECT MAX(total_laid_off)
					FROM layoffs_analysis
					)
;

-- ¿Qué empresas despidieron a toda su plantilla?
SELECT *
FROM layoffs_analysis
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Total de despidos por industria
SELECT 
	industry, 
    SUM(total_laid_off) AS total_off
FROM layoffs_analysis
GROUP BY industry
ORDER BY 2 DESC;

-- Total de despidos por país
SELECT 
	country, 
    SUM(total_laid_off) AS total_off
FROM layoffs_analysis
GROUP BY country
ORDER BY 2 DESC;

-- Total de despidos por año
SELECT 
	YEAR(`date`) AS `year`,
    SUM(total_laid_off) AS total_off
FROM layoffs_analysis
GROUP BY `year`
ORDER BY 1 DESC;

-- Total de despidos por mes

SELECT 
	LEFT(`date`, 7) AS `month`, 
	SUM(total_laid_off) AS total_off
FROM layoffs_analysis
WHERE LEFT(`date`, 7) IS NOT NULL
GROUP BY `month`
ORDER BY 1;

-- Total acumulado de despidos por mes

WITH monthly_totals AS
(
SELECT 
	LEFT(`date`, 7) AS `month`, 
	SUM(total_laid_off) AS monthly_sum
FROM layoffs_analysis
WHERE LEFT(`date`, 7) IS NOT NULL
GROUP BY `month`
ORDER BY 1
)
SELECT 
	*,
	SUM(monthly_sum) OVER(ORDER BY `month`) AS rolling_total
FROM monthly_totals;


-- ¿Cuáles fueron las 5 empresas que más empleados despidieron cada año?

WITH ranked_companies AS
(
SELECT 
	company, 
	YEAR(`date`) AS `year`, 
	SUM(total_laid_off) AS total_off,
    DENSE_RANK() OVER(PARTITION BY YEAR(`date`) ORDER BY SUM(total_laid_off) DESC) AS ranking
FROM layoffs_analysis
WHERE YEAR(`date`) IS NOT NULL
GROUP BY company, `year`
)
SELECT *
FROM ranked_companies
WHERE ranking <= 5;