WITH cellphone_number AS
(
    SELECT
        person_id,
        COALESCE(
            MAX(CASE WHEN person_attribute_type_id = 12 THEN value END),
            MAX(CASE WHEN person_attribute_type_id = 14 THEN value END),
            MAX(CASE WHEN person_attribute_type_id = 15 THEN value END)
        ) AS cellphone_number
    FROM person_attribute
    WHERE voided = 0
    GROUP BY person_id
)
SELECT
    p.person_id,
    (
        SELECT property_value
        FROM global_property
        WHERE property = 'current_health_center_id'
    ) AS site_id,
    MAX(pn.given_name) AS given_name,
    MAX(pn.middle_name) AS middle_name,
    MAX(pn.family_name) AS family_name,
    CASE
        WHEN UPPER(p.gender) IN ('M','MALE') THEN 'M'
        WHEN UPPER(p.gender) IN ('F','FEMALE') THEN 'F'
        ELSE 'Unknown'
    END AS sex,
    CASE
        WHEN YEAR(p.birthdate) < 1900 THEN '1900-01-01'
        ELSE p.birthdate
    END AS birthdate,
    CASE WHEN p.birthdate_estimated = 1 THEN 'Yes' ELSE 'Yes' END AS birthdate_est,
    c.cellphone_number,
    MAX(pa.region) AS region_of_origin,
    MAX(pa.address2) AS home_district,
    MAX(pa.county_district) AS home_traditional_authority,
    MAX(pa.neighborhood_cell) AS home_village,
    MAX(pa.region) AS region,
    MAX(pa.state_province) AS current_district,
    MAX(pa.township_division) AS current_traditional_authority,
    MAX(pa.city_village) AS current_village,
    MAX(paa.value) AS closest_landmark,
    GROUP_CONCAT(DISTINCT pi2.identifier) AS national_id
FROM person p
LEFT JOIN patient_program pp
    ON p.person_id = pp.patient_id
   AND pp.program_id = 1
   AND pp.voided = 0
LEFT JOIN person_name pn
    ON p.person_id = pn.person_id
   AND pn.voided = 0
LEFT JOIN person_address pa
    ON p.person_id = pa.person_id
   AND pa.voided = 0
LEFT JOIN person_attribute paa
    ON p.person_id = paa.person_id
   AND paa.voided = 0
   AND paa.person_attribute_type_id = 19
LEFT JOIN cellphone_number c
    ON p.person_id = c.person_id
LEFT JOIN patient_identifier pi2
    ON p.person_id = pi2.patient_id
   AND pi2.identifier_type = 28
   AND pi2.voided = 0
WHERE p.voided = 0
GROUP BY p.person_id;
 
