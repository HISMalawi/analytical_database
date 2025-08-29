with attribute_concepts as  
(
select
	'cd4_count' as attribute,
	'5497' as attribute_concepts
union all
select
	'allergic_to_cotrimaxole' as attribute,
	'8012' as attribute_concepts
union all
select
	'patient_present' as attribute,
	'1805' as attribute_concepts
union all
select
	'guardian_present' as attribute,
	'2122' as attribute_concepts
union all
select
	'visit_type' as attribute,
	'5315' as attribute_concepts
union all
select
	'weight' as attribute,
	'5089' as attribute_concepts
union all
select
	'height' as attribute,
	'5090' as attribute_concepts
union all
select
	'bmi' as attribute,
	'2137' as attribute_concepts
union all
select
	'systolic_blood_pressure' as attribute,
	'5085' as attribute_concepts
union all
select
	'diastolic_blood_pressure' as attribute,
	'5086' as attribute_concepts
union all
select
	'temperature' as attribute,
	'5088' as attribute_concepts
union all
select
	'blood_oxygen_saturation' as attribute,
	'5092' as attribute_concepts
union all
select
	'pulse' as attribute,
	'5087' as attribute_concepts
union all
select
	'patient_pregnant' as attribute,
	'1755,6131' as attribute_concepts
union all
select
	'patient_breastfeeding' as attribute,
	'834,7965' as attribute_concepts
union all
select
	'family_planning_method_currently_on' as attribute,
	'374' as attribute_concepts
union all
select
	'family_planning_method_provided_today' as attribute,
	'1618' as attribute_concepts
union all
select
	'reason_for_not_using_family_planning_method' as attribute,
	'6195' as attribute_concepts
union all
select
	'side_effects' as attribute,
	'5945,512,9440,2148,877,6029,3,107,215,219,620,821,867,1458,5978,5980,6408,8260,9242' as attribute_concepts
union all
select
	'tb_status' as attribute,
	'7459' as attribute_concepts
union all
select
	'on_tb_treatment' as attribute,
	'2690' as attribute_concepts
union all
select
	'date_started_treatment_known' as attribute,
	'1421' as attribute_concepts
union all
select
	'date_started_treatment' as attribute,
	'10687' as attribute_concepts
union all
select
	'routine_tb_screening' as attribute,
	'8259' as attribute_concepts
union all
select
	'doses_missed' as attribute,
	'2973' as attribute_concepts
union all
select
	'art_adherence' as attribute,
	'818,2681' as attribute_concepts
union all
select
	'reason_for_poor_adherence' as attribute,
	'1740,2686,3140' as attribute_concepts
union all
select
	'lab_test_type' as attribute,
	'9737' as attribute_concepts
union all
select
	'lab_reason_for_test' as attribute,
	'2429' as attribute_concepts
union all
select
	'lab_result' as attribute,
	'7363,2216' as attribute_concepts
union all
select
	'does_the_patient_have_hypertension' as attribute,
	'6414' as attribute_concepts
union all
select
	'date_hypertension_was_diagnosed' as attribute,
	'6415' as attribute_concepts
union all
select
	'htn_date_enrolled' as attribute,
	'8754' as attribute_concepts
union all
select
	'risk_factors' as attribute,
	'9500' as attribute_concepts
union all
select
	'hypertension_drugs_prescribed' as attribute,
	'9498' as attribute_concepts
union all
select
	'notes' as attribute,
	'5097' as attribute_concepts
),
obs_data as (
select
	o.person_id,
	o.concept_id,
	date(o.obs_datetime) visit_date,
	ac.`attribute`,
	cn.name AS concept_name,
	o.concept_id AS obs_concept_id,
	o.value_coded,
	cn2.name AS value_coded_value,
	o.value_coded_name_id,
	o.value_drug,
	o.value_datetime,
	o.value_modifier,
	o.value_numeric,
	o.value_text
from
	obs o
JOIN attribute_concepts ac ON
	FIND_IN_SET(o.concept_id, ac.attribute_concepts) > 0
LEFT JOIN concept_name cn ON
	o.concept_id = cn.concept_id
	AND cn.concept_name_type = 'FULLY_SPECIFIED'
	AND cn.locale = 'en'
	AND cn.voided = 0
LEFT JOIN concept_name cn2 ON
	o.value_coded = cn2.concept_id
	AND cn2.concept_name_type = 'FULLY_SPECIFIED'
	AND cn2.locale = 'en'
	AND cn2.voided = 0
),
final_pull as 
(
select
	distinct x.patient_id,
	x.visit_date,
	x.site_id
from
	(
	select
		distinct e.patient_id,
		date(e.encounter_datetime) visit_date,
		(
		select
			property_value site_id
		from
			global_property
		where
			property = 'current_health_center_id') site_id
	from
		obs o
	join encounter e on
		o.encounter_id = e.encounter_id
	join concept_name cn on
		cn.concept_id = o.concept_id
		and cn.voided = 0
		and cn.locale = 'en'
		and cn.concept_name_type = 'FULLY_SPECIFIED'
	where
		o.concept_id in
(
		select
			concept_id
		from
			concept_name
		where
			concept_id in (856, 2834)
				and voided = 0
				and locale = 'en'
				and concept_name_type = 'FULLY_SPECIFIED')
		and o.voided = 0
		and o.encounter_id is not null
		and o.person_id is not null
		and e.voided = 0
		and e.encounter_type in (54, 25, 57, 13, 10, 32)
			and COALESCE(o.value_text, o.value_numeric) is not null
				and o.value_drug in (
				select
					drug_id
				from
					arv_drug)
		union all
			select
				o.patient_id,
				date(o.start_date) visit_date,
				(
				select
					property_value site_id
				from
					global_property
				where
					property = 'current_health_center_id') site_id
			from
				orders o
			join obs ob on
				o.order_id = ob.order_id
				and ob.voided = 0
				and ob.concept_id = 9737
				and ob.value_coded = 856
				and o.order_type_id in (3, 4)
					and o.voided = 0
			union all
				select
					ob.person_id as patient_id,
					date(ob.obs_datetime) as visit_date,
					(
					select
						property_value site_id
					from
						global_property
					where
						property = 'current_health_center_id') site_id
				from
					obs ob
				join concept_name cn on
					ob.concept_id = cn.concept_id
				where
					ob.concept_id = 856
					and ob.voided = 0
					and ob.order_id is not null
					and coalesce(value_text, value_numeric) is not null
			union all
				select
					ob.person_id as patient_id,
					date(ob.obs_datetime) as visit_date,
					(
					select
						property_value site_id
					from
						global_property
					where
						property = 'current_health_center_id') site_id
				from
					obs ob
				join concept_name cn on
					ob.concept_id = cn.concept_id
				where
					ob.concept_id = 856
					and ob.voided = 0
					and ob.order_id is null
					and coalesce(value_text, value_numeric) is not null) x
),
final_dispensations AS (
select
	x.*
from
	(
with medication_regimen as (
	select
		GROUP_CONCAT(drug.drug_id order by drug.drug_id asc) as drugs,
		regimen_name.name as regimen_name,
		combo.regimen_combination_id
	from
		moh_regimen_combination combo
	inner join moh_regimen_combination_drug drug on
		combo.regimen_combination_id = drug.regimen_combination_id
	inner join moh_regimen_name regimen_name on
		combo.regimen_name_id = regimen_name.regimen_name_id
	group by
		combo.regimen_combination_id,
		regimen_name.name
),
	drug_qty as (
	select
		o.patient_id,
		DATE(o.start_date) as visit_date,
		d.drug_inventory_id,
		dd.name as drug_name,
		greatest(SUM(coalesce(d.quantity,ob.value_numeric)), 0) as total_quantity
	from
		orders o
	join drug_order d on
		o.order_id = d.order_id
	join encounter e on
		o.encounter_id = e.encounter_id
	join drug dd on
		d.drug_inventory_id = dd.drug_id
        join obs ob on o.order_id = ob.order_id
	where
		e.encounter_type in (54, 25)
			and e.voided = 0
			and o.voided = 0
                        and ob.voided = 0
			and d.drug_inventory_id in (
			select
				drug_id
			from
				arv_drug)
		group by
			o.patient_id,
			DATE(o.start_date),
			d.drug_inventory_id
),
	home_qty as (
	select
		o.person_id as patient_id,
		date(o.obs_datetime) as visit_date,
		do.drug_inventory_id,
		d.name as drug_name,
		greatest(SUM(coalesce(o.value_numeric, o.value_text, 0)), 0) as total_home
	from
		drug_order do
	join obs o on
		do.order_id = o.order_id
	join orders o2 on
		o2.order_id = do.order_id
	join drug d on
		d.drug_id = do.drug_inventory_id
	where
		o2.voided = 0
		and o.voided = 0
		and o.concept_id = 6781
		and do.drug_inventory_id in (
		select
			drug_id
		from
			arv_drug ad)
		and d.retired = 0
	group by
		o.person_id,
		date(o.obs_datetime),
		do.drug_inventory_id
	union all
select
	o.person_id as patient_id,
	date(o.obs_datetime) as visit_date,
	o.value_drug,
	d.name as drug_name,
	greatest(SUM(coalesce(o.value_numeric, o.value_text, 0)), 0) as total_clinic
from
	obs o
join drug d on
	o.value_drug = d.drug_id
where o.concept_id = 6781
	and d.drug_id in (select drug_id from arv_drug)
	and d.retired = 0
	and o.voided = 0
	and o.order_id is null
group by o.person_id, date(o.obs_datetime), o.value_drug
),
	clinic_qty as (
	select
		o.person_id as patient_id,
		date(o.obs_datetime) as visit_date,
		do.drug_inventory_id,
		d.name as drug_name,
		greatest(SUM(coalesce(o.value_numeric, o.value_text, 0)), 0) as total_clinic
	from
		drug_order do
	join obs o on
		do.order_id = o.order_id
	join orders o2 on
		o2.order_id = do.order_id
	join drug d on
		d.drug_id = do.drug_inventory_id
	where
		o2.voided = 0
		and o.voided = 0
		and o.concept_id = 2540
		and do.drug_inventory_id in (
		select
			drug_id
		from
			arv_drug ad)
		and d.retired = 0
	group by
		o.person_id,
		date(o.obs_datetime),
		do.drug_inventory_id
union all	
select
	o.person_id as patient_id,
	date(o.obs_datetime) as visit_date,
	o.value_drug,
	d.name as drug_name,
	greatest(SUM(coalesce(o.value_numeric, o.value_text, 0)), 0) as total_clinic
from
	obs o
join drug d on
	o.value_drug = d.drug_id
where o.concept_id = 2540
	and d.drug_id in (select drug_id from arv_drug)
	and d.retired = 0
	and o.voided = 0
      and o.order_id is null
group by o.person_id, date(o.obs_datetime), o.value_drug
),
	patient_visit_drugs as (
	select
		distinct
        o.patient_id,
		DATE(o.start_date) as visit_date,
		dd.name as drug_name,
		d.drug_inventory_id
	from
		orders o
	join drug_order d on
		o.order_id = d.order_id
	join encounter e on
		o.encounter_id = e.encounter_id
	join drug dd on
		d.drug_inventory_id = dd.drug_id
	join obs ob on
		ob.order_id = o.order_id
		and DATE(o.start_date) = DATE(ob.obs_datetime)
	where
		e.encounter_type in (54, 25)
			and e.voided = 0
			and o.voided = 0
			and dd.retired = 0
			and d.drug_inventory_id in (
			select
				drug_id
			from
				arv_drug)
			and coalesce(ob.value_numeric, ob.value_text, 0) > 0
),
	drug_combinations as (
	select
		pvd.patient_id,
		pvd.visit_date,
		GROUP_CONCAT(distinct pvd.drug_inventory_id order by pvd.drug_inventory_id asc) as drug_comb
	from
		patient_visit_drugs pvd
	group by
		pvd.patient_id,
		pvd.visit_date
)
	select
		pvd.patient_id,
		pvd.visit_date,
		dc.drug_comb,
		GROUP_CONCAT(distinct CONCAT(pvd.drug_name, ':', DATE(o.auto_expire_date))) as auto_expire_date,
		GROUP_CONCAT(distinct CONCAT(pvd.drug_name, ':', o.instructions) SEPARATOR '|') as instructions,
		GROUP_CONCAT(distinct CONCAT(pvd.drug_name, ':', do.equivalent_daily_dose)) as equivalent_daily_dose,
		GROUP_CONCAT(distinct CONCAT(q.drug_name, ':', q.total_quantity)) as quantity,
		GROUP_CONCAT(distinct CONCAT(hq.drug_name, ':', hq.total_home)) as art_pills_remaining_at_home,
		GROUP_CONCAT(distinct CONCAT(cq.drug_name, ':', cq.total_clinic)) as art_pills_remaining_brought_to_clinic,
		coalesce(mr.regimen_name, 'Unknown') as art_regimen,
		1 as dispensed
	from
		patient_visit_drugs pvd
	join orders o on
		o.patient_id = pvd.patient_id
		and DATE(o.start_date) = pvd.visit_date
	join drug_order do on
		o.order_id = do.order_id
			and do.drug_inventory_id = pvd.drug_inventory_id
		join drug_combinations dc on
			dc.patient_id = pvd.patient_id
			and dc.visit_date = pvd.visit_date
		left join medication_regimen mr on
			mr.drugs = dc.drug_comb
		left join drug_qty q on
			q.patient_id = pvd.patient_id
			and q.visit_date = pvd.visit_date
			and q.drug_inventory_id = pvd.drug_inventory_id
		left join home_qty hq on
			hq.patient_id = pvd.patient_id
			and hq.visit_date = pvd.visit_date
			and hq.drug_inventory_id = pvd.drug_inventory_id
		left join clinic_qty cq on
			cq.patient_id = pvd.patient_id
			and cq.visit_date = pvd.visit_date
			and cq.drug_inventory_id = pvd.drug_inventory_id
		group by
			pvd.patient_id,
			pvd.visit_date,
			dc.drug_comb,
			mr.regimen_name
) x ),
final_non_art_dispensations AS (
select
	x.*
from
	(
with drug_qty as (
	select
		o.patient_id,
		DATE(o.start_date) as visit_date,
		d.drug_inventory_id,
		dd.name as drug_name,
		greatest(SUM(coalesce(d.quantity,ob.value_numeric)), 0) as total_quantity
	from
		orders o
	join drug_order d on
		o.order_id = d.order_id
	join encounter e on
		o.encounter_id = e.encounter_id
	join drug dd on
		d.drug_inventory_id = dd.drug_id
        join obs ob on o.order_id = ob.order_id
	where
		e.encounter_type in (54, 25)
			and e.voided = 0
			and o.voided = 0
                        and ob.voided = 0
			and d.drug_inventory_id not in (
			select
				drug_id
			from
				arv_drug)
		group by
			o.patient_id,
			DATE(o.start_date),
			d.drug_inventory_id
),
	home_qty as (
	select
		o.person_id as patient_id,
		date(o.obs_datetime) as visit_date,
		do.drug_inventory_id,
		d.name as drug_name,
		greatest(SUM(coalesce(o.value_numeric, o.value_text, 0)), 0) as total_home
	from
		drug_order do
	join obs o on
		do.order_id = o.order_id
	join orders o2 on
		o2.order_id = do.order_id
	join drug d on
		d.drug_id = do.drug_inventory_id
	where
		o2.voided = 0
		and o.voided = 0
		and o.concept_id = 6781
		and do.drug_inventory_id not in (
		select
			drug_id
		from
			arv_drug ad)
		and d.retired = 0
	group by
		o.person_id,
		date(o.obs_datetime),
		do.drug_inventory_id
	union all
select
	o.person_id as patient_id,
	date(o.obs_datetime) as visit_date,
	o.value_drug,
	d.name as drug_name,
	greatest(SUM(coalesce(o.value_numeric, o.value_text, 0)), 0) as total_clinic
from
	obs o
join drug d on
	o.value_drug = d.drug_id
where o.concept_id = 6781
	and d.drug_id not in (select drug_id from arv_drug)
	and d.retired = 0
	and o.voided = 0
	and o.order_id is null
group by o.person_id, date(o.obs_datetime), o.value_drug
),
	clinic_qty as (
	select
		o.person_id as patient_id,
		date(o.obs_datetime) as visit_date,
		do.drug_inventory_id,
		d.name as drug_name,
		greatest(SUM(coalesce(o.value_numeric, o.value_text, 0)), 0) as total_clinic
	from
		drug_order do
	join obs o on
		do.order_id = o.order_id
	join orders o2 on
		o2.order_id = do.order_id
	join drug d on
		d.drug_id = do.drug_inventory_id
	where
		o2.voided = 0
		and o.voided = 0
		and o.concept_id = 2540
		and do.drug_inventory_id not in (
		select
			drug_id
		from
			arv_drug ad)
		and d.retired = 0
	group by
		o.person_id,
		date(o.obs_datetime),
		do.drug_inventory_id
union all	
select
	o.person_id as patient_id,
	date(o.obs_datetime) as visit_date,
	o.value_drug,
	d.name as drug_name,
	greatest(SUM(coalesce(o.value_numeric, o.value_text, 0)), 0) as total_clinic
from
	obs o
join drug d on
	o.value_drug = d.drug_id
where o.concept_id = 2540
	and d.drug_id not in (select drug_id from arv_drug)
	and d.retired = 0
	and o.voided = 0
	and o.order_id is null
group by o.person_id, date(o.obs_datetime), o.value_drug
),
	patient_visit_drugs as (
	select
		distinct
        o.patient_id,
		DATE(o.start_date) as visit_date,
		dd.name as drug_name,
		d.drug_inventory_id
	from
		orders o
	join drug_order d on
		o.order_id = d.order_id
	join encounter e on
		o.encounter_id = e.encounter_id
	join drug dd on
		d.drug_inventory_id = dd.drug_id
	join obs ob on
		ob.order_id = o.order_id
		and DATE(o.start_date) = DATE(ob.obs_datetime)
	where
		e.encounter_type in (54, 25)
			and e.voided = 0
			and o.voided = 0
			and dd.retired = 0
			and d.drug_inventory_id not in (
			select
				drug_id
			from
				arv_drug)
			and coalesce(ob.value_numeric, ob.value_text, 0) > 0
),
	drug_combinations as (
	select
		pvd.patient_id,
		pvd.visit_date,
		GROUP_CONCAT(distinct pvd.drug_inventory_id order by pvd.drug_inventory_id asc) as drug_comb
	from
		patient_visit_drugs pvd
	group by
		pvd.patient_id,
		pvd.visit_date
)
	select
		pvd.patient_id,
		pvd.visit_date,
		dc.drug_comb,
		GROUP_CONCAT(distinct CONCAT(pvd.drug_name, ':', DATE(o.auto_expire_date))) as auto_expire_date,
		GROUP_CONCAT(distinct CONCAT(pvd.drug_name, ':', o.instructions) SEPARATOR '|') as instructions,
		GROUP_CONCAT(distinct CONCAT(pvd.drug_name, ':', do.equivalent_daily_dose)) as equivalent_daily_dose,
		GROUP_CONCAT(distinct CONCAT(q.drug_name, ':', q.total_quantity)) as quantity,
		GROUP_CONCAT(distinct CONCAT(hq.drug_name, ':', hq.total_home)) as other_pills_remaining_at_home,
		GROUP_CONCAT(distinct CONCAT(cq.drug_name, ':', cq.total_clinic)) as other_pills_remaining_brought_to_clinic,
		1 as dispensed
	from
		patient_visit_drugs pvd
	join orders o on
		o.patient_id = pvd.patient_id
		and DATE(o.start_date) = pvd.visit_date
	join drug_order do on
		o.order_id = do.order_id
			and do.drug_inventory_id = pvd.drug_inventory_id
		join drug_combinations dc on
			dc.patient_id = pvd.patient_id
			and dc.visit_date = pvd.visit_date
		left join drug_qty q on
			q.patient_id = pvd.patient_id
			and q.visit_date = pvd.visit_date
			and q.drug_inventory_id = pvd.drug_inventory_id
		left join home_qty hq on
			hq.patient_id = pvd.patient_id
			and hq.visit_date = pvd.visit_date
			and hq.drug_inventory_id = pvd.drug_inventory_id
		left join clinic_qty cq on
			cq.patient_id = pvd.patient_id
			and cq.visit_date = pvd.visit_date
			and cq.drug_inventory_id = pvd.drug_inventory_id
		group by
			pvd.patient_id,
			pvd.visit_date,
			dc.drug_comb
) x
),
visit_appointments as 
(
SELECT
	o.person_id,
	pp.visit_date,
	DATE(o.value_datetime) AS appointment_date
FROM
	obs o
INNER JOIN (
	SELECT
		o.person_id,
		DATE(o.obs_datetime) AS visit_date,
		MAX(o.obs_id) AS obs_id
	FROM
		obs o
	WHERE
		o.concept_id IN (5096)
			AND o.voided = 0
		GROUP BY
			o.person_id,
			DATE(o.obs_datetime)
        ) pp ON
	pp.obs_id = o.obs_id
),
art_adherence as  
(
SELECT
	o.person_id,
	date(o.obs_datetime) visit_date,
	o.order_id,
	do.drug_inventory_id,
	dr.name drug_name,
	o.value_numeric,
	o.value_text,
	o.value_modifier,
	concat(dr.name, ':', coalesce(o.value_numeric, o.value_text, ''), coalesce(o.value_modifier, '')) art_adherence
FROM
	obs o
join orders oo on
	o.order_id = oo.order_id
	and o.voided = 0
	and oo.voided = 0
join drug_order do on
	oo.order_id = do.order_id
join drug dr on
	do.drug_inventory_id = dr.drug_id
join arv_drug ad on
	do.drug_inventory_id = ad.drug_id
WHERE
	o.concept_id = 6987
),
lab_tests_data as
(
SELECT
	distinct xx.*
from
	(
WITH
test_type_concepts AS (
	SELECT
		cn.concept_id,
		name
	FROM
		concept_name cn
	INNER JOIN concept cc
			USING (concept_id)
	WHERE
		cn.name = 'Test type'
		AND cc.retired = 0
		AND cn.voided = 0
),
	lab_test_result_concepts AS (
	SELECT
		cn.concept_id,
		name
	FROM
		concept_name cn
	INNER JOIN concept cc
			USING (concept_id)
	WHERE
		cn.name = 'Lab test result'
		AND cc.retired = 0
		AND cn.voided = 0
),
	specimen_type_concepts AS (
	SELECT
		concept_id,
		name
	FROM
		concept_name
	WHERE
		name IN ('Blood', 'DBS (Free drop to DBS card)', 'DBS (Using capillary tube)',
                   'Plasma', 'Sputum', 'Cerebrospinal fluid', 'Urine', 'Unknown')
			AND voided = 0
),
	lab_order_types AS (
	SELECT
		order_type_id
	FROM
		order_type
	WHERE
		name IN ('Lab', 'Test')
			AND retired = 0
),
	reason_for_testing AS (
	SELECT
		DISTINCT ob.person_id,
		o.order_id,
		DATE(ob.obs_datetime) visit_date,
		COALESCE(cn.name, ob.value_text) reason_for_testing
	FROM
		orders o
	LEFT JOIN obs ob ON
		o.encounter_id = ob.encounter_id
	LEFT JOIN concept_name cn ON
		ob.value_coded = cn.concept_id
		AND cn.concept_name_type = 'FULLY_SPECIFIED'
		AND cn.voided = 0
		AND cn.locale = 'en'
	WHERE
		o.order_type_id = 4
		AND ob.concept_id IN (2429, 10609, 10610)
			AND ob.voided = 0
			AND o.voided = 0
			AND COALESCE(cn.name, ob.value_text) IS NOT NULL
)
	SELECT
		DISTINCT
    o.patient_id,
		rft.reason_for_testing AS lab_reason_for_test,
		DATE(o.start_date) AS lab_order_test_date,
		test_type.name AS lab_test_type,
		DATE(vl_obs.obs_datetime) AS lab_result_date,
		COALESCE(vl_obs.value_modifier, '=') AS result_modifier,
		COALESCE(vl_obs.value_numeric, vl_obs.value_text) AS `result`,
		CONCAT(' ', COALESCE(vl_obs.value_modifier, '='), COALESCE(vl_obs.value_numeric, vl_obs.value_text)) AS lab_result,
		'' AS results_test_facility,
		specimen_type.name AS sample_type,
		'' AS sending_facility
	FROM
		orders o
	LEFT JOIN reason_for_testing rft
    ON
		rft.person_id = o.patient_id
		AND rft.order_id = o.order_id
		AND rft.visit_date = DATE(o.start_date)
	LEFT JOIN specimen_type_concepts specimen_type
    ON
		specimen_type.concept_id = o.concept_id
	LEFT JOIN obs test_obs
    ON
		test_obs.order_id = o.order_id
			AND test_obs.concept_id IN (
			SELECT
				concept_id
			FROM
				test_type_concepts)
			AND test_obs.voided = 0
		LEFT JOIN concept_name test_type
    ON
			test_obs.value_coded = test_type.concept_id
			AND test_type.concept_name_type = 'FULLY_SPECIFIED'
			AND test_type.locale = 'en'
			AND test_type.voided = 0
		LEFT JOIN obs vl_obs
    ON
			vl_obs.order_id = o.order_id
			AND vl_obs.concept_id = 856
			AND vl_obs.voided = 0
		LEFT JOIN person p ON
			p.person_id = o.patient_id
			AND p.voided = 0
		WHERE
			o.order_type_id IN (
			SELECT
				order_type_id
			FROM
				lab_order_types)
			AND o.voided = 0
			AND (vl_obs.obs_id IS NOT NULL
				OR o.concept_id IS NOT NULL)
	UNION ALL
		SELECT
			DISTINCT
    o.person_id AS patient_id,
			coalesce(rft.reason_for_testing, NULL) AS lab_reason_for_test,
			DATE(o.obs_datetime) as lab_order_test_date,
			'Viral Load' AS lab_test_type,
			DATE(o.obs_datetime) AS lab_result_date,
			COALESCE(o.value_modifier, '=') AS result_modifier,
			COALESCE(o.value_numeric, o.value_text) AS `result`,
			CONCAT(' ', COALESCE(o.value_modifier, '='), COALESCE(o.value_numeric, o.value_text)) AS lab_result,
			'' AS results_test_facility,
			'' AS sample_type,
			'' AS sending_facility
		FROM
			obs o
		LEFT JOIN reason_for_testing rft
    ON
			rft.person_id = o.person_id
			AND rft.order_id = o.order_id
		JOIN encounter e ON
			o.encounter_id = e.encounter_id
		JOIN concept_name cn ON
			cn.concept_id = o.concept_id
			AND cn.concept_name_type = 'FULLY_SPECIFIED'
			AND cn.locale = 'en'
			AND cn.voided = 0
		WHERE
			o.concept_id = 856
			AND o.voided = 0
			AND o.order_id IS NULL
			AND o.person_id IS NOT NULL
			AND COALESCE(o.value_text, o.value_numeric) IS NOT NULL
				AND
  COALESCE(o.value_text, o.value_numeric) != ''
					AND e.voided = 0
					AND e.encounter_type IN (57, 13)
) xx
where
	xx.`result` is null
	or xx.`result` NOT IN ('<', '>', '=', '')
		and xx.lab_order_test_date is not null
)
select
	distinct
fp.patient_id,
	fp.site_id,
	fp.visit_date,
	case
		when 
	max(case when od.`attribute` = 'patient_present' then od.value_coded_value else NULL end) is not null
		and max(case when od.`attribute` = 'patient_present' then od.value_coded_value else NULL end) not in ('yes', 'no') then 'unknown'
		else max(case when od.`attribute` = 'patient_present' then od.value_coded_value else NULL end)
	end as patient_present,
	case
		when 
	max(case when od.`attribute` = 'guardian_present' then od.value_coded_value else NULL end) is not null
		and max(case when od.`attribute` = 'guardian_present' then od.value_coded_value else NULL end) not in ('yes', 'no') then 'unknown'
		else max(case when od.`attribute` = 'guardian_present' then od.value_coded_value else NULL end)
	end as guardian_present,
	CASE
		WHEN COALESCE(NULLIF(MAX(CASE WHEN od.`attribute` = 'patient_present' THEN od.value_coded_value END), ''), 'unknown') = 'yes' THEN 'patient'
		WHEN COALESCE(NULLIF(MAX(CASE WHEN od.`attribute` = 'patient_present' THEN od.value_coded_value END), ''), 'unknown') = 'no'
		OR COALESCE(NULLIF(MAX(CASE WHEN od.`attribute` = 'patient_present' THEN od.value_coded_value END), ''), 'unknown') IS NULL
		AND COALESCE(NULLIF(MAX(CASE WHEN od.`attribute` = 'guardian_present' THEN od.value_coded_value END), ''), 'unknown') = 'yes' THEN 'guardian'
		ELSE null
	END as visit_type,
		max(case when od.`attribute` = 'weight' then coalesce(od.value_numeric, od.value_text) else NULL end) as weight,
		max(case when od.`attribute` = 'height' then coalesce(od.value_numeric, od.value_text) else NULL end) as height,
		max(case when od.`attribute` = 'bmi' then coalesce(od.value_numeric, od.value_text) else NULL end) as bmi,
		max(case when od.`attribute` = 'systolic_blood_pressure' then coalesce(od.value_numeric, od.value_text) else NULL end) as systolic_blood_pressure,
		max(case when od.`attribute` = 'diastolic_blood_pressure' then coalesce(od.value_numeric, od.value_text) else NULL end) as diastolic_blood_pressure,
		max(case when od.`attribute` = 'temperature' then coalesce(od.value_numeric, od.value_text) else NULL end) as temperature,
		max(case when od.`attribute` = 'blood_oxygen_saturation' then coalesce(od.value_numeric, od.value_text) else NULL end) as blood_oxygen_saturation,
		max(case when od.`attribute` = 'pulse' then coalesce(od.value_numeric, od.value_text) else NULL end) as pulse,
		max(case when od.`attribute` = 'patient_pregnant' then od.value_coded_value else NULL end) as patient_pregnant,
		max(case when od.`attribute` = 'patient_breastfeeding' then od.value_coded_value else NULL end) as patient_breastfeeding,
		group_concat(distinct case when od.`attribute` = 'family_planning_method_currently_on' then od.value_coded_value else NULL end) as family_planning_method_currently_on,
		group_concat(distinct case when od.`attribute` = 'family_planning_method_provided_today' then od.value_coded_value else NULL end) as family_planning_method_provided_today,
		max(case when od.`attribute` = 'reason_for_not_using_family_planning_method' then od.value_text else NULL end) as reason_for_not_using_family_planning_method,
		GROUP_CONCAT( DISTINCT IF(od.attribute = 'side_effects' AND od.value_coded_value IN ('Yes'), od.concept_name, NULL )) AS side_effects,
		max(case when od.`attribute` = 'on_tb_treatment' then od.value_coded_value else NULL end) on_tb_treatment,
		max(case when od.`attribute` = 'tb_status' then od.value_coded_value else NULL end) as tb_status,
		case
		when (max(case when od.`attribute` = 'date_started_treatment' then od.value_datetime else NULL end)) is not null then 'yes'
		else null
	end as date_started_treatment_known,
		max(case when od.`attribute` = 'date_started_treatment' then date(od.value_datetime) else NULL end) as date_started_treatment,
		max(case when od.`attribute` = 'routine_tb_screening' then coalesce(od.value_coded_value, od.value_text) else NULL end) as routine_tb_screening,
		max(case when od.`attribute` = 'cd4_count' then coalesce(od.value_coded_value, od.value_text) else NULL end) as cd4_count,
		max(case when od.`attribute` = 'allergic_to_cotrimaxole' then coalesce(od.value_coded_value, od.value_text) else NULL end) as allergic_to_cotrimaxole,
		fd.quantity as art_treatment_dispensed,
		fd.equivalent_daily_dose as dosage_on_art_treatment,
		fd.auto_expire_date as art_treatment_auto_expire_date,
		fd.instructions as art_treatment_instructions_given,
		fd.art_regimen regimen_category,
		fnd.quantity as other_drugs_dispensed,
		fnd.equivalent_daily_dose as dosage_on_non_art_treatment,
		fnd.auto_expire_date as other_drugs_auto_expire_date,
		fnd.instructions as other_drugs_instructions_given,
		va.appointment_date next_appointment_date,
		fd.art_pills_remaining_brought_to_clinic,
		fd.art_pills_remaining_at_home,
		fnd.other_pills_remaining_brought_to_clinic,
		fnd.other_pills_remaining_at_home,
		max(case when od.`attribute` = 'doses_missed' then od.value_numeric else NULL end) as doses_missed,
		group_concat( distinct aa.art_adherence) art_adherence,
		max(case when od.`attribute` = 'reason_for_poor_adherence' then coalesce(od.value_coded_value, od.value_text) else NULL end) as reason_for_poor_adherence,
		ltd.lab_order_test_date,
		group_concat( distinct case when ltd.lab_test_type is not null and ltd.lab_test_type != '' then ltd.lab_test_type else null end ) lab_test_type,
		group_concat(distinct ltd.lab_reason_for_test) lab_reason_for_test,
		TRIM(LEADING ',' FROM GROUP_CONCAT(DISTINCT CASE WHEN ltd.lab_result_date IS NOT NULL THEN ltd.lab_result_date ELSE NULL END)) AS lab_result_date,
		group_concat( distinct concat(ltd.lab_result_date, ':', ltd.lab_result)) lab_result,
		group_concat( distinct case when ltd.sample_type is not null and ltd.sample_type != '' then ltd.sample_type else null end ) sample_type
from
		final_pull fp
left join
obs_data od on
		(fp.patient_id,
		fp.visit_date)=(od.person_id,
		od.visit_date)
left join final_dispensations fd on
		(fp.patient_id,
		fp.visit_date)=(fd.patient_id,
		fd.visit_date)
left join final_non_art_dispensations fnd on
		(fp.patient_id,
		fp.visit_date)=(fnd.patient_id,
		fnd.visit_date)
left join visit_appointments va on
		(fp.patient_id,
		fp.visit_date)=(va.person_id,
		va.visit_date)
left join art_adherence aa on
		(fp.patient_id,
		fp.visit_date)=(aa.person_id,
		aa.visit_date)
left join lab_tests_data ltd ON
		fp.patient_id = ltd.patient_id
	and date(fp.visit_date)= date(ltd.lab_order_test_date)
group by
		fp.patient_id,
		fp.visit_date;
