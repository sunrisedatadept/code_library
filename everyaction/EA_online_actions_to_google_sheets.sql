-- all form responses
WITH responses AS (
    SELECT
	contacts_forms.contactsonlineformid,
	contacts_forms.onlineformid,
	contacts_forms.vanid,
	contacts_forms.submittedfirstname as firstname,
	contacts_forms.submittedlastname as lastname,
	contacts_forms.submittedmobilephone as phone,
	contacts_forms.submittedhomeemail as email,
	contacts_forms.submittedcity as city,
	contacts_forms.submittedstateprovince as state,
	contacts_forms.submittedpostalcode as zip

    FROM sunrise_ea.tsm_tmc_contactsonlineforms_sm AS contacts_forms

    LEFT JOIN sunrise_ea.tsm_tmc_onlineforms_sm AS forms
        ON contacts_forms.onlineformid = forms.onlineformid

    WHERE forms.onlineformid = 113 -- this identifies the collegiate form
),

-- boolean responses
boolean_responses AS (
    SELECT
        responses.contactsonlineformid,
        questions.onlineformid,
        responses.onlineformquestionid AS questionid,
        questions.onlineformquestionname AS question_name,
        responses.responsevalue AS response

    FROM sunrise_ea.tsm_tmc_contactsonlineformsresponses_boolean_sm AS responses

    LEFT JOIN sunrise_ea.tsm_tmc_onlineformquestions_sm AS questions
        ON responses.onlineformquestionid = questions.onlineformquestionid

    WHERE questions.onlineformid = 443
),

-- pivot boolean responses
pivot_boolean AS (
    SELECT
        contactsonlineformid,

        SUM(CASE
            WHEN question_name ilike '%I will be a student during the fall%' THEN response
            ELSE 0
        END) AS is_student_and_is_excited_to_defeat_trump,

        SUM(CASE
            WHEN question_name ilike '%I understand that successful completion%' THEN response
            ELSE 0
        END) AS does_understand_all_six_trainings

    FROM boolean_responses

    GROUP BY 1
),

-- longtext responses
longtext_responses AS (
    SELECT
        responses.contactsonlineformid,
        questions.onlineformid,
        responses.onlineformquestionid AS questionid,
        questions.onlineformquestionname AS question_name,
        responses.responsevalue AS response

    FROM sunrise_ea.tsm_tmc_contactsonlineformsresponses_longtext_sm AS responses

    LEFT JOIN sunrise_ea.tsm_tmc_onlineformquestions_sm AS questions
        ON responses.onlineformquestionid = questions.onlineformquestionid

    WHERE questions.onlineformid = 443
),

-- pivot longtext responses
pivot_longtext AS (
    SELECT
        contactsonlineformid,

        MAX(CASE
            WHEN question_name ilike '%How would you try to convince a friend to vote%' THEN response
            ELSE NULL
        END) AS convince_a_friend,

        MAX(CASE
            WHEN question_name ilike '%How will your life or the lives of the people%' THEN response
            ELSE NULL
        END) AS friends_lives_impacted,

        MAX(CASE
            WHEN question_name ilike '%Once we defeat Trump, what would you tell a friend%' THEN response
            ELSE NULL
        END) AS resistance_over,

        MAX(CASE
            WHEN question_name ilike '%Anything else%' THEN response
            ELSE NULL
        END) AS anything_else

    FROM longtext_responses
    GROUP BY 1
),

-- single option responses
single_responses AS (
    SELECT
        responses.contactsonlineformid,
        questions.onlineformid,
        responses.onlineformquestionid AS questionid,
        questions.onlineformquestionname AS question_name,
        -- note: this is different to the other patterns
        formresponses.onlineformresponsename AS response

    FROM sunrise_ea.tsm_tmc_contactsonlineformsresponses_single_sm AS responses

    LEFT JOIN sunrise_ea.tsm_tmc_onlineformquestions_sm AS questions
        ON responses.onlineformquestionid = questions.onlineformquestionid

    LEFT JOIN sunrise_ea.tsm_tmc_onlineformresponses_sm AS formresponses
        ON responses.onlineformresponseid = formresponses.onlineformresponseid

    WHERE questions.onlineformid = 443
),

-- pivot single option repsonses
pivot_single AS (
    SELECT
        contactsonlineformid,

        MAX(CASE
            WHEN question_name ilike '%College or University%' THEN response
            ELSE NULL
        END) AS college,

        MAX(CASE
            WHEN question_name ilike '%School Type%' THEN response
            ELSE NULL
        END) AS school_type,

        MAX(CASE
            WHEN question_name ilike '%school state%' THEN response
            ELSE NULL
        END) AS school_state

    FROM single_responses

    GROUP BY 1
),

-- date responses
date_responses AS (
    SELECT
        responses.contactsonlineformid,
        questions.onlineformid,
        responses.onlineformquestionid AS questionid_date,
        questions.onlineformquestionname AS question_name,
        responses.responsevalue AS response

    FROM sunrise_ea.tsm_tmc_contactsonlineformsresponses_date_sm AS responses

    LEFT JOIN sunrise_ea.tsm_tmc_onlineformquestions_sm AS questions
        ON responses.onlineformquestionid = questions.onlineformquestionid

    WHERE questions.onlineformid = 443
),

-- pivot date responses
pivot_date AS (
    SELECT
        contactsonlineformid,

        MAX(CASE
            WHEN question_name ilike '%Fall Semester Start Date%' THEN response
            ELSE NULL
        END) AS fall_semester_start_date

    FROM date_responses

    GROUP BY 1
),

-- integer responses
integer_responses AS (
    SELECT
        responses.contactsonlineformid,
        questions.onlineformid,
        responses.onlineformquestionid AS questionid_date,
        questions.onlineformquestionname AS question_name,
        responses.responsevalue AS response

    FROM sunrise_ea.tsm_tmc_contactsonlineformsresponses_integer_sm AS responses

    LEFT JOIN sunrise_ea.tsm_tmc_onlineformquestions_sm AS questions
        ON responses.onlineformquestionid = questions.onlineformquestionid

    WHERE questions.onlineformid = 443
),

-- pivot integer repsonses
pivot_integer AS (
    SELECT
        contactsonlineformid,

        MAX(CASE
            WHEN question_name ilike '%Expected Graduation Year%' THEN response
            ELSE NULL
        END) AS expected_graduation_year

    FROM integer_responses

    GROUP BY 1
),

-- shorttext responses
shorttext_responses AS (
    SELECT
        responses.contactsonlineformid,
        questions.onlineformid,
        responses.onlineformquestionid AS questionid_date,
        questions.onlineformquestionname AS question_name,
        responses.responsevalue AS response

    FROM sunrise_ea.tsm_tmc_contactsonlineformsresponses_shorttext_sm AS responses

    LEFT JOIN sunrise_ea.tsm_tmc_onlineformquestions_sm AS questions
        ON responses.onlineformquestionid = questions.onlineformquestionid

    WHERE questions.onlineformid = 443
),

-- pivot shorttext repsonses
pivot_shorttext as(
    SELECT
        contactsonlineformid,

        MAX(CASE
            WHEN question_name ilike '%First Name 1%' THEN response
            ELSE NULL
        END) AS friend_1,

        MAX(CASE
            WHEN question_name ilike '%First Name 2%' THEN response
            ELSE NULL
        END) AS friend_2,

        MAX(CASE
            WHEN question_name ilike '%First Name 3%' THEN response
            ELSE NULL
        END) AS friend_3,

        MAX(CASE
            WHEN question_name ilike '%school zip%' THEN response
            ELSE NULL
        END) AS school_zip

    FROM shorttext_responses

    GROUP BY 1
),

-- one record per contactsonlineformid
pivoted_responses AS (
    SELECT
        responses.contactsonlineformid,
        responses.vanid,
        responses.firstname,
        responses.lastname,
	responses.email,
	responses.phone,
	responses.city,
	responses.state,
	responses.zip,
        pivot_single.college,
        pivot_single.school_type,
        pivot_single.school_state,
        pivot_shorttext.school_zip,
        pivot_integer.expected_graduation_year,
        pivot_date.fall_semester_start_date,
        pivot_boolean.is_student_and_is_excited_to_defeat_trump,
        pivot_boolean.does_understand_all_six_trainings,
        pivot_longtext.friends_lives_impacted,
        pivot_longtext.convince_a_friend,
        pivot_longtext.resistance_over,
        pivot_longtext.anything_else,
        pivot_shorttext.friend_1,
        pivot_shorttext.friend_2,
        pivot_shorttext.friend_3,
        ROW_NUMBER() OVER (PARTITION BY responses.vanid ORDER BY responses.contactsonlineformid DESC) = 1 AS is_most_recent

    FROM responses

    LEFT JOIN pivot_single
        ON responses.contactsonlineformid = pivot_single.contactsonlineformid

    LEFT JOIN pivot_longtext
        ON responses.contactsonlineformid = pivot_longtext.contactsonlineformid

    LEFT JOIN pivot_boolean
        ON responses.contactsonlineformid = pivot_boolean.contactsonlineformid

    LEFT JOIN pivot_date
        ON responses.contactsonlineformid = pivot_date.contactsonlineformid

    LEFT JOIN pivot_integer
        ON responses.contactsonlineformid = pivot_integer.contactsonlineformid

    LEFT JOIN pivot_shorttext
        ON responses.contactsonlineformid = pivot_shorttext.contactsonlineformid

),

-- one record per vanid
most_recent_response AS (
    SELECT * FROM pivoted_responses
    WHERE is_most_recent
),

recent_xfields_base AS (
    SELECT
        xfields.vanid,

        MAX(date_updated::date) AS max_date

    FROM sunrise.contacts_extra_fields AS xfields

    GROUP BY xfields.vanid
),

recent_xfields AS (
    SELECT  -- Note to future self: investigate this table
        xfields.vanid,
        xfields.race,
        xfields.gender,
        xfields.class,
        xfields.dob

    FROM sunrise.contacts_extra_fields AS xfields

    INNER JOIN recent_xfields_base
        ON xfields.vanid = recent_xfields_base.vanid
        AND xfields.date_updated::date = recent_xfields_base.max_date::date
),



final AS (
-- assemble final table
    SELECT
        most_recent_response.vanid,
        most_recent_response.firstname,
        most_recent_response.lastname,
        recent_xfields.race,
        recent_xfields.gender,
        recent_xfields.dob,
        recent_xfields.class,
        most_recent_response.city,
        most_recent_response.state,
        most_recent_response.zip,
        most_recent_response.phone,
        most_recent_response.email,
        most_recent_response.college,
        most_recent_response.school_type,
        most_recent_response.school_state,
        most_recent_response.school_zip,
        most_recent_response.expected_graduation_year,
        most_recent_response.fall_semester_start_date,
        most_recent_response.is_student_and_is_excited_to_defeat_trump,
        most_recent_response.does_understand_all_six_trainings,
        most_recent_response.friends_lives_impacted,
        most_recent_response.convince_a_friend,
        most_recent_response.resistance_over,
        most_recent_response.anything_else,
        most_recent_response.friend_1,
        most_recent_response.friend_2,
        most_recent_response.friend_3,

        GETDATE() AS updated_date

    FROM most_recent_response
    LEFT JOIN recent_xfields
        ON most_recent_response.vanid = recent_xfields.vanid
)

SELECT * FROM final
