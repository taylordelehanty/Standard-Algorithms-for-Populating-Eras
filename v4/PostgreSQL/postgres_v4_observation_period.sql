/* Builds observation_period from observation
 * Orders everything by person_id then observation_date
 * Then reverses the order, and takes the MAX(ord) to get the end date
 * Takes the smallest and largest dates pertaining to one person's observation
 * giving us the start_ and end_dates of the observation
 */

TRUNCATE <schema>.observation_period;

WITH cteObservationTarget(person_id, observation_date) AS (
	SELECT
		person_id
		, observation_date
	FROM <schema>.observation
)

, cteOrderOfObservation(person_id, observation_date/*, count*/) AS (

	SELECT
		person_id
		, observation_date
		--, rev_ord
	FROM (
	    SELECT
		person_id
		, observation_date
		, ord
		, MAX(ord) OVER (PARTITION BY person_id ORDER BY observation_date DESC) as rev_ord
		--Do DESC to get the reverse order
	    FROM (
		SELECT
		    person_id
		    , observation_date
		    , ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY observation_date) AS ord
		FROM cteObservationTarget
		ORDER BY person_id, observation_date
	    ) RAWDATA 
	) e
	WHERE e.ord = 1 OR e.ord = e.rev_ord
)
/*
SELECT
    person_id
    , observation_date
FROM cteOrderOfObservation
ORDER BY person_id, observation_date;
*/
, cteObservationPeriod(person_id, observation_period_start_date, observation_period_end_date/*, observation_count*/) AS (
	SELECT
		oo.person_id
		, MIN(oo.observation_date) AS observation_period_start_date
		, MAX(oo.observation_date) AS observation_period_end_date
		--, count AS observation_count
	FROM cteOrderOfObservation oo
	JOIN cteObservationTarget ot
	ON oo.person_id = ot.person_id
	GROUP BY
		oo.person_id
		--, oo.count
)

INSERT INTO <schema>.observation_period(person_id, observation_period_start_date, observation_period_end_date)
SELECT person_id, observation_period_start_date, observation_period_end_date/*, observation_count*/
FROM cteObservationPeriod
ORDER BY person_id;


/*
--This is a test that to make sure records weren't lost through the process
--Uncomment all of the inner-code comments to test the count check
SELECT
	( SELECT COUNT(*) FROM <schema>.observation) AS beforecode
	, ( SELECT SUM(observation_count) FROM cteObservationPeriod) AS aftercode
*/
