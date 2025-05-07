SELECT 
    p.patient_id,
    (SELECT property_value FROM global_property WHERE property='current_health_center_id') AS site_id,
    pi2.identifier AS arv_number,
    MAX(CASE WHEN o.concept_id = 2552 THEN cn2.name ELSE '' END) AS follow_up_agreement,
    MAX(CASE WHEN o.concept_id = 7754 THEN cn2.name ELSE NULL END) AS ever_received_art,
    MAX(CASE WHEN o.concept_id = 7880 THEN cn2.name ELSE NULL END) AS confirmatory_test_type,
    MAX(CASE WHEN o.concept_id = 7881 THEN o.value_text ELSE NULL END) AS confirmatory_test_location,
    MAX(CASE WHEN o.concept_id = 7882 THEN CAST(o.value_datetime AS DATE) ELSE NULL END) AS confirmatory_test_date,
    MAX(CASE WHEN o.concept_id = 7751 THEN CAST(o.value_datetime AS DATE) ELSE NULL END) AS date_art_last_taken,
    MAX(CASE WHEN o.concept_id = 6394 THEN cn2.name ELSE NULL END) AS taken_arvs_last_2_weeks,
    MAX(CASE WHEN o.concept_id = 7752 THEN cn2.name ELSE NULL END) AS taken_arvs_last_2_months,
    MAX(CASE WHEN o.concept_id = 7937 THEN cn2.name ELSE NULL END) AS ever_registered_at_art_clinic,
    MAX(CASE WHEN o.concept_id = 7750 THEN o.value_text ELSE NULL END) AS location_of_art_initiation,
    MAX(CASE WHEN o.concept_id = 2516 THEN CAST(o.value_datetime AS DATE) ELSE NULL END) AS art_start_date,
    MIN(CAST(ps.start_date AS DATE)) AS start_date_estimated,
    CAST(pp.date_enrolled AS DATE) AS date_enrolled_at_facility,
    TIMESTAMPDIFF(YEAR, pe.birthdate, MIN(ps.start_date)) AS age_at_initiation,
    TIMESTAMPDIFF(DAY, pe.birthdate, MIN(ps.start_date)) AS age_in_days_at_initiation,
    MAX(CASE WHEN o.concept_id = 6981 THEN o.value_text ELSE NULL END) AS art_number_at_previous_location,
    MAX(CASE WHEN o.concept_id = 7879 THEN o.value_text ELSE NULL END) AS hts_linkage_number,
    MAX(CASE WHEN o.concept_id = 6393 THEN cn2.name ELSE NULL END) AS has_transfer_letter,
    MAX(CONCAT(' ', o.value_modifier, o.value_numeric)) AS cd4_count
FROM 
    patient p
JOIN 
    encounter e ON p.patient_id = e.patient_id AND e.encounter_type = 9 AND e.voided = 0
JOIN 
    obs o ON e.encounter_id = o.encounter_id AND o.voided = 0
LEFT JOIN 
    concept_name cn ON o.concept_id = cn.concept_id AND cn.locale = 'en' AND cn.voided = 0 AND cn.concept_name_type = 'FULLY_SPECIFIED'
LEFT JOIN 
    concept_name cn2 ON o.value_coded = cn2.concept_id AND cn2.locale = 'en' AND cn2.voided = 0 AND cn2.concept_name_type = 'FULLY_SPECIFIED'
LEFT JOIN 
    patient_identifier pi2 ON p.patient_id = pi2.patient_id AND pi2.identifier_type = 4 AND pi2.voided = 0
LEFT JOIN 
    patient_program pp ON p.patient_id = pp.patient_id AND pp.program_id = 1 AND pp.voided = 0
LEFT JOIN 
    patient_state ps ON pp.patient_program_id = ps.patient_program_id AND ps.voided = 0 AND DATE(ps.start_date) >= '1900-01-01'
LEFT JOIN 
    person pe ON p.patient_id = pe.person_id
GROUP BY 
    p.patient_id, pi2.identifier, pp.date_enrolled, pe.birthdate;
