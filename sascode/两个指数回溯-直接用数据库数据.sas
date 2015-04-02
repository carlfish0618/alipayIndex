/***�ð汾ֱ���������ݿ⣬���л��ݡ���***/

/*** !!! ��Ҫ�����У����ó��� **/


/************************************** �Խ����ɷֹɼ��÷� ************/
/** �������� */
PROC SQL;
	CREATE TABLE adjust_busdate AS
	SELECT date AS end_date LABEL "end_date"
	FROM busday
	GROUP BY year(date), month(date)
	HAVING date = max(date);
QUIT;
DATA adjust_busdate;
	SET adjust_busdate;
	IF "15dec2008"d <= end_date <= "28feb2015"d;
RUN;

/** Step1- ƥ������ */
/** Step1-1: ����300��Ʊ�� + ��֤500��Ʊ�� + ��ҵ��Ϣ�� */
%MACRO get_nearby_date(from_table, data_table , to_table);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*,
		  	 datepart(B.end_date) AS end_date_b FORMAT yymmdd10.
		FROM &from_table. A
		LEFT JOIN 
		(SELECT distinct end_date
		 FROM &data_table.
		 )B
		ON A.end_date >= datepart(B.end_date)
		GROUP BY A.end_date
		HAVING B.end_date = max(B.end_date)
		ORDER BY A.end_date;
	QUIT;
	DATA &to_table.;
		SET tmp;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND get_nearby_date;

%get_nearby_date(adjust_busdate, tinysoft.fg_uni_hs300, adjust_busdate_f);
DATA adjust_busdate_f;
	SET adjust_busdate_f;
	RENAME end_date_b = hs300_date;
RUN;
%get_nearby_date(adjust_busdate_f, tinysoft.fg_uni_csi500, adjust_busdate_f);
DATA adjust_busdate_f;
	SET adjust_busdate_f;
	RENAME end_date_b = csi500_date;
RUN;
%get_nearby_date(adjust_busdate_f, bk.fg_wind_sector, adjust_busdate_f);
DATA adjust_busdate_f;
	SET adjust_busdate_f;
	RENAME end_date_b = sector_date;
RUN;

/** Step1-2: ȡ��ѡ�سɷֹ� */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.end_date, A.sector_date,  B.stock_code
	FROM adjust_busdate_f A LEFT JOIN tinysoft.fg_uni_hs300 B
	ON A.hs300_date = datepart(B.end_date)
	ORDER BY A.end_date;
QUIT;
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.end_date, A.sector_date, B.stock_code
	FROM adjust_busdate_f A LEFT JOIN tinysoft.fg_uni_csi500 B
	ON A.csi500_date = datepart(B.end_date)
	ORDER BY A.end_date;
QUIT;
PROC SQL;
	CREATE TABLE fg_taobao_st_score AS
	SELECT distinct A.end_date, A.stock_code
	FROM 
	(SELECT * FROM tmp UNION SELECT * FROM tmp2) A
	LEFT JOIN bk.fg_wind_sector B
	ON A.sector_date = datepart(B.end_date) AND A.stock_code = B.stock_code
	WHERE o_code = "FG08"   /** ҽҩ��ҵ **/
	ORDER BY end_date, stock_code;
QUIT;

/** �޳������������Ĺ�Ʊ */
DATA fg_taobao_st_score;
	SET fg_taobao_st_score;
	IF stock_code IN ('600851', '002614', '600490', '300061') THEN delete;
	IF stock_code = '600849' AND end_date >= "28feb2010"d THEN delete;
RUN;

PROC SORT DATA = fg_taobao_st_score;
	BY end_date stock_code;
RUN;

/*** Step2�� �޳������������Ĺ�Ʊ **/
%filter_stock_mdf(input_pool=fg_taobao_st_score, output_pool=fg_taobao_st_score);


/*** Step3: �����Ա��÷� */ 
/** Step3-1: �Ա���ҵƥ��**/
%gen_stock_mapping(taobao_medicine_20150330.xlsx, med_map, TAOBAO_MEDICINE);
%gen_mapping_extend(adjust_table = adjust_busdate, input_table = med_map, start_date=15dec2008, 
		end_date=28feb2015, output_table=med_map, is_filter = 0);
%get_indus_name(med_map, med_map, TAOBAO_MEDICINE, is_reverse = 1);

PROC SORT DATA = med_map;
	BY end_date stock_code;
RUN;


/** ���ĺ��Ľ�����жԱ� */
/* !!���ģ��߼���ȷ��������δ�������У���Ҫ��ͣ�ƹ�Ʊ�޳���*/
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT B.end_date, B.stock_code, B.indus_code, A.indus_weight,*/
/*		A.stock_code AS stock_code_a, A.indus_code AS indus_code_a, A.indus_weight AS indus_weight_a, a.end_date AS end_date_a*/
/*	FROM med_map A FULL JOIN */
/*	(SELECT * FROM fgtest.fg_taobao_map WHERE fund_name = "TAOBAO_MEDICINE") B*/
/*	ON A.end_date = datepart(B.end_date) AND A.stock_code = B.stock_code AND A.indus_code = B.indus_code */
/*	WHERE missing(B.stock_code) OR abs(A.indus_weight -B.indus_weight)>=0.01 OR missing(A.stock_code);*/
/*QUIT;*/

/* step3-2 �Ա���Ʊ���У����ɵ÷� */
DATA med_map(drop = stock_code rename =(stock_code_b = stock_code));
	SET med_map;
	end_date = dhms(end_date,0,0,0);
	FORMAT end_date datetime20.;
	LENGTH stock_code_b $ 12;
	stock_code_b = stock_code;
RUN;

%gen_taobao_score(med_map, fgtest.fg_taobao_rawdata, TAOBAO_MEDICINE,  output_table=taobao_med_score);
PROC SORT DATA = taobao_med_score;
	BY end_date stock_code;
RUN;

DATA fg_taobao_st_score;
	SET fg_taobao_st_score;
	end_date = dhms(end_date,0,0,0);
	FORMAT end_date datetime20.;
RUN;

PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.indus_count, B.taobao_score
	FROM fg_taobao_st_score A LEFT JOIN taobao_med_score B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;

DATA fg_taobao_st_score;
	SET tmp;
	IF missing(indus_count) THEN indus_count = 0;
RUN;

PROC SQL;
	CREATE TABLE sub_raw_score AS
	SELECT *
	FROM product.fg_raw_score
	WHERE stock_code IN 
		(SELECT stock_code FROM fg_taobao_st_score);
QUIT;

PROC SQL;
	CREATE TABLE fg_taobao_st_score2 AS
	SELECT A.*, B.fmv_sqr 
	FROM fg_taobao_st_score A LEFT JOIN sub_raw_score B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;

%normalize_single_score(input_table=fg_taobao_st_score2, colname=taobao_score, output_table=fg_taobao_st_score2, is_replace = 1);
DATA fg_taobao_st_score;
	SET fg_taobao_st_score2(drop = fmv_sqr);
RUN;

/** ���ĺ�������Ա� */
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT B.end_date, B.stock_code, B.taobao_score, B.indus_count,*/
/*		A.stock_code AS stock_code_a, a.end_date AS end_date_a, A.taobao_score AS taobao_score_a, A.indus_count AS indus_count_a*/
/*	FROM fg_taobao_st_score A FULL JOIN */
/*	(SELECT * FROM fgtest.fg_taobao_st_score) B*/
/*	ON A.end_date = B.end_date AND A.stock_code = B.stock_code */
/*	WHERE missing(B.stock_code) OR  missing(A.stock_code) OR A.indus_count ~= B.indus_count OR abs(A.taobao_score - B.taobao_score) >= 0.001;*/
/*QUIT;*/

/** �ĺ��汾����(�Ѹ���)��
1- ͣ�ƹ�Ʊû���޳���������Ϊuniverse����Ҫ�޳���
2- ��ҵͳ���ϣ���ʹû�е÷֣�Ҳ��Ҫ������ҵ���������⣬û����ҵƥ��ģ���ҵһ���趨Ϊ0��ԭʼ�汾�趨Ϊ1����
**/ 

/*** Step4- ������ɸ����÷� **/
PROC SQL;
	CREATE TABLE fg_taobao_st_score_med AS
	SELECT A.end_date, A.stock_code, fmv_sqr,. AS tot,crateps_gg,crateps_jt,updneps_gg,updneps_jt,cratsale_gg,cratsale_jt,
		updnsale_gg,updnsale_jt,cratprofit_jt,updnprofit_jt,
			rec_gg,rec_jt,tp2p_gg,tp2p_jt,
			V_PE_t01,V_PE_t12,V_PE_ttm,V_PEE,V_PFV,V_PB_t01,V_PB_t12,V_PB_ttm,V_d_PE_t01,V_d_PP_jt,
			Q_accrual,Q_cfce,Q_roe_t1,Q_roe_t2,
			Sur_eps,Sur_sale,Sur_pre_eps,
			M_turnover_120,M_turnover_90,M_turnover_60,M_turnover_20,M_moneystrength,M_momentum,M_reversal,M_sigma,
			Gro_eps_t01,Gro_eps_t12,Gro_peg
    FROM fg_taobao_st_score A LEFT JOIN sub_raw_score B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;

/**��׼���÷֡�*/
%normalize_multi_score(input_table=fg_taobao_st_score_med, output_table = fg_taobao_st_score_med2, exclude_list=("TOT", "FMV_SQR"));

/** ���ĺ��Ա� **/
%LET cmp_var = CRATEPS_JT;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT end_date, stock_code, &cmp_var., fmv_sqr
	FROM fg_taobao_st_score_med
	WHERE end_Date = dhms("28nov2014"d,0,0,0)
	ORDER BY stock_code;
QUIT;

DATA tt;
	SET fg_taobao_st_score_med2;
	IF missing(fmv_sqr) OR fmv_sqr = 0;
RUN;


PROC SQL;
	CREATE TABLE stat AS
	SELECT B.end_date, B.stock_code, B.&cmp_var.,
		A.stock_code AS stock_code_a, a.end_date AS end_date_a, A.&cmp_var. AS &cmp_var._a
	FROM fg_taobao_st_score_med2 A FULL JOIN 
	(SELECT * FROM fgtest.fg_taobao_st_score_med) B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code 
	WHERE missing(B.stock_code) OR  missing(A.stock_code) OR abs(A.&cmp_var. - B.&cmp_var.) >=0.01;
QUIT;


/** ����tot�ܷ� */
/** ĿǰȨ��ֻ��һ���汾 */
DATA fg_taobao_st_score_med(drop = i);
	SET fg_taobao_st_score_med2;
	ARRAY varlist(42) CRATEPS_GG--GRO_PEG;
	DO i = 1 TO 42;
		IF missing(varlist(i)) THEN varlist(i) = 0;
	END;
RUN;

%multi_factor_score(input_table=fg_taobao_st_score_med, output_table = fg_taobao_st_score_med, exclude_list=("TOT", "FMV_SQR"));
PROC SQL NOPRINT;
	SELECT sum(weight) 
		INTO :sum_weight
	FROM fgtest.fg_score_weight 
	WHERE fsector = "FG4";
QUIT;
%put &sum_weight.;
DATA fg_taobao_st_score_med;
	SET fg_taobao_st_score_med;
	tot = sum(of CRATEPS_GG--GRO_PEG)/&sum_weight.;
RUN;




/** ��tot��׼�� */
%normalize_single_score(input_table=fg_taobao_st_score_med, colname=tot, output_table=fg_taobao_st_score_med, is_replace = 1);
PROC SQL;
	CREATE TABLE fg_taobao_st_score2 AS
	SELECT A.end_date, A.stock_code, A.indus_count, A.taobao_score, B.tot AS fg_score LABEL "fg_score"
	FROM fg_taobao_st_score A LEFT JOIN fg_taobao_st_score_med B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA fg_taobao_st_score;
	SET fg_taobao_st_score2;
RUN;

/** Step5- �����ۺϵ÷� */
%cal_tot(input_table=fg_taobao_st_score, v_tbpct=0.2, output_table=fg_taobao_st_score);



	


/****���ĺ��ĶԱȡ�***/
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.end_date AS end_date_b, B.stock_code AS stock_code_b,
		B.indus_count AS indus_count_b, B.fg_score AS fg_score_b, B.taobao_score AS taobao_score_b, 
		B.tot_score AS tot_score_b
	FROM fg_taobao_st_score A FULL JOIN fgtest.fg_taobao_st_score B
	ON A.end_Date = B.end_Date AND A.stock_code = B.stock_code
	WHERE missing(B.stock_code) OR  missing(A.stock_code) OR abs(A.tot_score - B.tot_score) >=0.001
	ORDER BY A.end_Date, A.stock_code;
QUIT;


/************************************** �����ֳɷֹɼ��÷� ************/
/** �������� */
PROC SQL;
	CREATE TABLE adjust_busdate AS
	SELECT date AS end_date LABEL "end_date"
	FROM busday
	GROUP BY year(date), month(date)
	HAVING date = max(date);
QUIT;
DATA adjust_busdate;
	SET adjust_busdate;
	IF "15jan2011"d <= end_date <= "28feb2015"d;
RUN;

/** Step0: ����mapping **/
%gen_stock_mapping(taobao_entertain_20150330.xlsx, ent_map, TAOBAO_ENTERTAIN);
%gen_mapping_extend(adjust_table = adjust_busdate, input_table = ent_map, start_date=15jan2011, 
		end_date=28feb2015, output_table=ent_map, is_filter = 0);
%get_indus_name(ent_map, ent_map, TAOBAO_ENTERTAIN, is_reverse = 1);

PROC SORT DATA = ent_map;
	BY end_date stock_code;
RUN;


/** Step1: ����universe���޳������������Ĺ�Ʊ����Ҫ�˹���Ԥ��*/
%filter_stock_mdf(input_pool=ent_map, output_pool=ent_uni);

DATA ent_uni;
	SET ent_uni;
	end_date = dhms(end_Date,0,0,0);
	FORMAT end_Date datetime20.;
RUN;

/** ���ĺ��汾���жԱ� */
/*PROC SQL;*/
/*	CREATE TABLE tmp AS*/
/*	SELECT A.end_Date, A.stock_code, A.indus_name, B.end_date AS end_date_b, B.stock_code AS stock_code_b*/
/*	FROM ent_uni A FULL JOIN */
/*	(SELECT **/
/*	FROM fgtest.fg_taobao_uni*/
/*	WHERE fund_name = "TAOBAO_ENTERTAIN") B*/
/*	ON A.end_Date = B.end_Date AND A.stock_code = B.stock_code */
/*	WHERE missing(B.stock_code) OR  missing(A.stock_code) */
/*	ORDER BY A.end_date, A.stock_code;*/
/*QUIT;*/


PROC SQL;
	CREATE TABLE sub_raw_score_ent AS
	SELECT *
	FROM product.fg_raw_score
	WHERE stock_code IN 
		(SELECT stock_code FROM ent_map);
QUIT;

/** Step2: ���㸻�����ӵ÷� **/
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.fmv_sqr, B.updnrec_gg, B.crateps_jt, B.sur_pre_eps
	FROM ent_uni A LEFT JOIN sub_raw_score_ent B
	ON A.end_Date = B.end_Date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.indus_code;
QUIT;
DATA fg_taobao_st_score_ent;
	SET tmp;
RUN;


/* ���������ӱ�׼�� */
%normalize_multi_score(input_table=fg_taobao_st_score_ent, output_table = fg_taobao_st_score_ent, 
	exclude_list=("FMV_SQR", "INDUS_CODE", "INDUS_WEIGHT", "INDUS_NAME", "STOCK_CODE", "STOCK_NAME", "FUND_NAME"));

/** �۳���ҵ���� */
%LET fname = updnrec_gg;

PROC SQL;
	CREATE TABLE indus_score AS
	SELECT end_date, indus_code, indus_name, sum(&fname.*fmv_sqr)/sum(fmv_sqr*(not missing(&fname.)* (not missing(fmv_sqr)))) AS &fname.,
		sum(&fname.*fmv_sqr)/sum(fmv_sqr) AS &fname._m,  
		sum(not missing(&fname.)* (not missing(fmv_sqr))) / count(1) AS pct, 
		count(1) AS nobs
	FROM fg_taobao_st_score_ent
	GROUP BY end_date, indus_code, indus_name
	ORDER BY pct;
QUIT;
DATA tt;
	SET indus_score;
	IF indus_name = "��������";
RUN;
PROC SORT DATA = tt;
	BY end_date;
RUN;

PROC SQL;
	CREATE TABLE indus_score AS
	SELECT end_date, 
		sum(not missing(&fname.)* (not missing(fmv_sqr))) / count(1) AS pct, 
		count(1) AS nobs
	FROM fg_taobao_st_score_ent
	GROUP BY end_date
	ORDER BY end_date;
QUIT;



PROC SQL;
	CREATE TABLE fg_taobao_ind_score AS
	SELECT end_date, indus_code, indus_name, 
		sum(updnrec_gg*fmv_sqr)/sum(fmv_sqr) AS updnrec_gg,  
		sum(crateps_jt*fmv_sqr)/sum(fmv_sqr) AS crateps_jt,
		sum(sur_pre_eps*fmv_sqr)/sum(fmv_sqr) AS sur_pre_eps
	FROM fg_taobao_st_score_ent
	GROUP BY end_date, indus_code, indus_name
	ORDER BY end_Date, indus_code;
QUIT;
DATA fg_taobao_ind_score(drop = updnrec_gg crateps_jt sur_pre_eps);
	SET fg_taobao_ind_score;
	fg_score = (updnrec_gg+crateps_jt+sur_pre_eps)/3;
RUN;

/** Step3: �����Ա����� */
PROC SQL;
	CREATE TABLE taobao_score AS
	SELECT end_Date, indus_code, data_value AS taobao_score
	FROM fgtest.fg_taobao_rawdata
	WHERE fund_name = "TAOBAO_ENTERTAIN"
	ORDER BY end_Date;
QUIT;

/** �����׼�� **/
PROC SQL;
	CREATE TABLE tmp As
	SELECT A.*, B.multiplier 
	FROM taobao_score A LEFT JOIN
	(SELECT end_date, max(abs(taobao_score)) AS multiplier
	FROM taobao_score
	GROUP BY end_Date) B
	ON A.end_Date = B.end_Date
	ORDER BY A.end_Date, indus_code;
QUIT;
DATA taobao_score(drop = multiplier);
	SET tmp;
	taobao_score= max(taobao_score/multiplier,0);
	LABEL taobao_score = "taobao_score";
RUN;

DATA fg_taobao_ind_score;
	UPDATE fg_taobao_ind_score taobao_score;
	BY end_Date indus_code;
RUN;

/** Step4: �����ۺϵ÷� */
%cal_tot(input_table=fg_taobao_ind_score, v_tbpct=0.5, output_table=fg_taobao_ind_score);



/** ���ĺ��Ľ��жԱ� */
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT A.end_date, A.indus_code, A.tot_score, B.tot_score AS tot_score_b*/
/*	FROM fg_taobao_ind_score A LEFT JOIN fgtest.fg_taobao_ind_score B*/
/*	ON A.end_date = B.end_Date AND A.indus_code = B.indus_code*/
/*	WHERE abs(A.tot_score - B.tot_score) >= 0.01 OR missing(A.tot_score) OR missing(B.tot_score)*/
/*	ORDER BY A.end_date, A.indus_code;*/
/*QUIT;*/
