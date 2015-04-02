/******************* ģ��4-2: ����ָ���ӱ�ѡ����ɸѡ���ɷݹ� ************/
%LET improve_pct = 0;
/** �ز����� **/
PROC SQL;
	CREATE TABLE adjust_busdate AS
	SELECT date AS end_date LABEL "end_date"
	FROM busday
	GROUP BY year(date), month(date)
	HAVING date = max(date);
QUIT;
DATA adjust_busdate;
	SET adjust_busdate;
	IF "15sep2014"d <= end_date <= "15feb2015"d;
RUN;
/**/
/*DATA sub_test(rename = (end_date = date));*/
/*	SET product.taobao_score;*/
/*	weight = 1;*/
/*	IF fund_name = "TAOBAO_ENTERTAIN";*/
/*RUN;*/

/**�����ĺ��汾����Ҫȡ���ɵ÷֡�*/
PROC SQL;
	CREATE TABLE sub_test AS
	SELECT A.*, B.tot_score
	FROM fgtest.fg_taobao_index A LEFT JOIN fgtest.fg_taobao_map C
	ON A.effective_date = C.end_date AND A.stock_code = C.stock_code AND C.fund_name = "TAOBAO_ENTERTAIN"
	LEFT JOIN fgtest.fg_taobao_ind_score B
	ON A.effective_date = B.end_date AND C.indus_code = B.indus_code 
	WHERE A.fund_name = "TAOBAO_ENTERTAIN" 
	ORDER BY A.effective_date, A.stock_code;
QUIT;

/*DATA tt;*/
/*	SET fgtest.fg_taobao_index;*/
/*	IF fund_name = "TAOBAO_ENTERTAIN";*/
/*RUN;*/
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT effective_date, count(1) AS nobs*/
/*	FROM fgtest.fg_taobao_index*/
/*	WHERE fund_name = "TAOBAO_ENTERTAIN"*/
/*	GROUP BY effective_date*/
/*	ORDER BY effective_date;*/
/*QUIT;*/


DATA sub_test(rename = (effective_date = date));
	SET sub_test;
	weight = 1;
	effective_date = datepart(effective_date);
	FORMAT effective_date yymmdd10.;
RUN;

PROC SORT DATA = sub_test;
	BY date stock_code;
RUN;



/** 1- ��׼���� **/
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.freeshare, C.close
	FROM sub_test A LEFT JOIN fg_wind_freeshare B
	ON A.stock_code = B.stock_code AND A.date = datepart(end_date)
	LEFT JOIN hqinfo C
	ON A.stock_code = C.stock_code AND A.date = C.end_date
	ORDER BY A.date, A.stock_code;
QUIT;

DATA test_stock_pool(drop = freevalue freeshare close);
	SET tmp;
	freevalue = freeshare * close;
	IF not missing(freevalue) THEN weight = freevalue;
	ELSE weight = 0;
RUN;


%gen_adjust_pool(stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, move_date_forward=0, output_stock_pool=test_stock_pool);
%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);

DATA base_stock_pool;
	SET test_stock_pool;
RUN;
PROC SORT DATA = base_stock_pool;
	BY end_date;
RUN;


/** 2. ��ǿ���� **/
%LET mark_table = score_half;
%LET is_equal =0;
%LET fname = tot_score;

/** ������������ע�÷��Ƿ����λ��ǰ50%��ҵ */
/*DATA factors_value;*/
/*	SET product.taobao_score_i;*/
/*	IF fund_name = "TAOBAO_ENTERTAIN";*/
/*RUN;*/

DATA factors_value;
	SET fgtest.fg_taobao_ind_score;
	end_Date = datepart(end_Date);
	FORMAT end_Date datetime20.;
RUN;
/* ȡindus_name��Ϣ */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.indus_name
	FROM factors_value A LEFT JOIN fgtest.fg_taobao_indus B
	ON A.indus_code = B.indus_code AND B.fund_name = "TAOBAO_ENTERTAIN"
	ORDER BY A.end_date, B.indus_name;
QUIT;
DATA factors_value;
	SET tmp;
RUN;



PROC SORT DATA = factors_value OUT = factors_value;
	BY end_date;
RUN;

PROC UNIVARIATE DATA = factors_value NOPRINT;
	BY end_date;
	VAR tot_score;
	OUTPUT OUT = tmp median = tot_score_m;
RUN;
PROC SQL;
	CREATe TABLE score_half AS
	SELECT A.end_date, A.indus_name, A.indus_code, 
		A.tot_score, B.tot_score_m
	FROM factors_value A LEFT JOIN tmp B
	ON A.end_date = B.end_date
	ORDER BY A.end_date, A.indus_name;
QUIT;
DATA score_half(keep = end_date indus_code indus_name tot_score);
	SET score_half;
	ARRAY var_list(1) tot_score;
	ARRAY var_list_m(1) tot_score_m;
	DO i = 1 TO 1;
		IF not missing(var_list(i)) AND var_list(i) > 0 AND var_list(i)>var_list_m(i) THEN var_list(i) = 1;
		ELSE var_list(i) = 0;
	END;
RUN;

/** ȡ��ҵ��Ϣ */
/*PROC SQL;*/
/*	CREATE TABLE test_stock_pool AS*/
/*	SELECT A.*, B.indus_code, B.indus_name, C.&fname AS &fname._score*/
/*	FROM sub_test A LEFT JOIN product.taobao_mapping B*/
/*	ON A.date = B.end_date AND A.stock_code = B.stock_code*/
/*	LEFT JOIN &mark_table. C*/
/*	ON A.date = C.end_date AND B.indus_code = C.indus_code*/
/*	WHERE B.fund_name = "TAOBAO_ENTERTAIN"*/
/*	ORDER BY A.date, B.indus_code;*/
/*QUIT;*/

PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT A.*, B.indus_code, C.&fname AS &fname._score, D.indus_name
	FROM sub_test A LEFT JOIN fgtest.fg_taobao_map B
	ON A.date = datepart(B.end_date) AND A.stock_code = B.stock_code AND B.fund_name = "TAOBAO_ENTERTAIN"
	LEFT JOIN &mark_table. C
	ON A.date = C.end_date AND B.indus_code = C.indus_code
	LEFT JOIN fgtest.fg_taobao_indus D
	ON B.indus_code = D.indus_code AND B.fund_name = "TAOBAO_ENTERTAIN"
	ORDER BY A.date, B.indus_code;
QUIT;



PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.freeshare, C.close
	FROM test_stock_pool A LEFT JOIN fg_wind_freeshare B
	ON A.stock_code = B.stock_code AND A.date = datepart(end_date)
	LEFT JOIN hqinfo C
	ON A.stock_code = C.stock_code AND A.date = C.end_date
	ORDER BY A.date, A.stock_code;
QUIT;

DATA test_stock_pool(drop = freevalue freeshare close);
	SET tmp;
	freevalue = freeshare * close;
	IF not missing(freevalue) THEN weight = freevalue;
	ELSE weight = 0;
RUN;


%gen_adjust_pool(stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, move_date_forward=0, output_stock_pool=test_stock_pool);
%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
	/** ��ҵ��ǿ */
PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.indus_wt, C.indus_score, C.indus_equal, D.sum_indus_score, D.sum_indus_equal
		FROM test_stock_pool A LEFT JOIN
		(SELECT end_date, indus_name, sum(weight) AS indus_wt
		FROM test_stock_pool
		GROUP BY end_date, indus_name) B
		ON A.end_date = B.end_date AND A.indus_name = B.indus_name
		LEFT JOIN
		(SELECT end_date, indus_name, 
		mean(&fname._score * &fname.) AS indus_score, /* �÷ּ�Ȩ */
		mean(&fname._score) AS indus_equal /* ��Ȩ */
		FROM test_stock_pool
		GROUP BY end_date, indus_name) C
		ON A.end_date = C.end_date AND A.indus_name = C.indus_name
		LEFT JOIN
		(SELECT end_date, 
		sum(&fname._score * &fname.) AS sum_indus_score, 
		sum(&fname._score) AS sum_indus_equal 
		FROM test_stock_pool
		GROUP BY end_date) D
		ON A.end_date = D.end_date
		ORDER BY A.end_date, A.indus_name;
QUIT;

	/** ��ǿ */
	DATA test_stock_pool;
		SET tmp;
		IF &is_equal. = 1 THEN DO;  /* ��Ȩ */
			IF sum_indus_equal = 0 THEN DO;  /* û��ѡ���κ���ҵ���Ͱ��ջ�׼�� */
				adj_indus_wt = indus_wt;
				multiplier = 1;
				adj_weight = weight * multiplier;
			END;
			ELSE DO;
				adj_indus_wt = indus_equal;
				multiplier = adj_indus_wt / indus_wt;
				adj_weight = weight * multiplier;
			END;
		END;
		ELSE IF &is_equal. = 0 THEN DO;
			IF sum_indus_score = 0 THEN DO;  /* û��ѡ���κ���ҵ���Ͱ��ջ�׼�� */
				adj_indus_wt = indus_wt;
				multiplier = 1;
				adj_weight = weight * multiplier;
			END;
			ELSE DO;
				adj_indus_wt = indus_score; /* �÷ּ�Ȩ */
				multiplier = adj_indus_wt / indus_wt;
				adj_weight = weight * multiplier;
			END;
		END;
		/** ����Ȩ�� */
		weight = adj_weight;
	RUN;
	/** ���µ�����Ȩ�أ�����Ȩ��Ϊ1 */
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
DATA im_stock_pool;
	SET test_stock_pool;
RUN;
PROC SORT DATA = im_stock_pool;
	BY end_date;
RUN;


/** ��׼+��ǿ */

PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT A.stock_code, A.end_date, A.effective_date, A.weight AS base_wt, B.weight AS im_wt
	FROM base_stock_pool A LEFT JOIN im_stock_pool B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA test_stock_pool(drop = im_wt base_wt);
	SET test_stock_pool;
	weight = base_wt * (1-&improve_pct.) + im_wt * &improve_pct.;
	IF weight = 0 THEN delete;
RUN;
PROC SORT DATA = test_stock_pool;
	BY end_date stock_code;
RUN;


/** ���⼫����������й�Ʊ��im_wt��Ϊ0 **/
/** ��ʵ�������Ҳ���Բ��á���Ϊ����ǿ�����趨�ˣ����û��ѡ����ҵ�򻹰��ջ�׼���á�**/
%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);

/* ���Ƹ���Ȩ�� */
%limit_adjust_stock_only(stock_pool=test_stock_pool, stock_upper=0.1, stock_lower=0, output_stock_pool=test_stock_pool);

DATA entertain_pool;
	SET test_stock_pool;
	LENGTH fund_name $32. ;
	fund_name = "TAOBAO_ENTERTAIN";
RUN;
PROC SORT DATA = entertain_pool;
	BY end_date descending weight;
RUN;
DATA product.taobao_stock_pool;
	SET entertain_pool;
RUN;
