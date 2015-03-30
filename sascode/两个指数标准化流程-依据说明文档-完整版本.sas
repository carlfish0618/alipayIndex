/** �ʺ�������ָ���Ĵ **/
/** ����˵��: 
(1) ��һ��ģ�鴴�����ֹ�ά�����������֮
(2) �ڶ���ģ�飬�����������ݴ���������׼���������������
    ����ʽ���е�ʱ�򣬸ò�����Ҫ�������µ����������������д���� ***/

%LET product_dir = D:\Research\������\����-��Ʒ;
%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

%LET taobao_dir = D:\Research\������;
LIBNAME taobao "&taobao_dir.\sasdata";
%INCLUDE "D:\Research\������\sascode\ָ������-��׼��\ָ��������׼��.sas";
options validvarname=any; /* ֧�����ı����� */

/*********************** ׼���������ݱ����� *******************/
%LET env_start_date = 31dec2008;

/*** (1) ���������б� */
PROC SQL;
	CREATE TABLE busday AS
	SELECT datepart(end_date) AS date FORMAT mmddyy10. LABEL "date"
	FROM
	(
	SELECT distinct(end_date)
	FROM taobao.hqinfo
	WHERE end_date >= dhms("&env_start_date."d, 0,0,0)
	)
	ORDER BY date;
QUIT;


/* (2) ����� */
PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT datepart(end_date) AS end_date FORMAT yymmdd10. LABEL "end_date", stock_code, close, factor, pre_close
	FROM taobao.hqinfo
	WHERE type = 'A' AND end_date >= dhms("&env_start_date."d, 0,0,0)
	ORDER BY end_date, stock_code;
QUIT;

/** (3) ������ͨ��ֵ�� */
PROC SQL;
	CREATE TABLE fg_wind_freeshare AS
	SELECT stock_code, end_date, freeshare
	FROM taobao.fg_wind_freeshare_neat;
QUIT;


/******************* ģ��1: ���ֹ�ά��������� ************/
/** ��1: taobao_indus_code: ��ҵ�������ҵ���ӳ���ϵ **/

/* ���������ҵ�������ҵ���ƥ�� */
/** ����汾���ṩ������İ汾��ȫһ�¡�֮���Ըð汾Ϊ׼ */
/** ҽҩ����: ҽ����е��ҽ�Ʒ��� */
PROC IMPORT OUT = product.taobao_indus_code
            DATAFILE= "&input_dir.\indus_list.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="indus_list$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
DATA product.taobao_indus_code(drop = indus_name2 fund_name2);
	SET product.taobao_indus_code(rename = (indus_name = indus_name2 fund_name = fund_name2));
	into_time = dhms(into_time,0,0,0);
	LENGTH indus_name $ 32;
	LENGTH fund_name $ 32;
	indus_name = trim(indus_name2);
	fund_name = trim(fund_name2);
	FORMAT into_time datetime20.;
RUN;

/** ��2: taobao_mapping ***/
/** 1.����ָ���ɷݹ� */
%MACRO gen_stock_mapping(input_file, output_table, fund_name);
	PROC IMPORT OUT = data_raw
            DATAFILE= "&input_dir.\&input_file." 
            DBMS=EXCEL REPLACE;
     	RANGE="��Ʊ���������ƥ��$"; 
     	GETNAMES=YES;
     	MIXED=NO;
     	SCANTEXT=NO;
     	USEDATE=YES;
     	SCANTIME=YES;
	RUN;

/** Step1: ������ҵƥ����Ϊ��ͬ���� */
DATA data_raw data_edit(drop = fg_level2_name i rename = (fg_level2_name_update = fg_level2_name));
	SET data_raw;
	DO i = 1 TO 20;
		fg_level2_name_update = scan(fg_level2_name, i, '/');
		IF not missing(fg_level2_name_update) THEN OUTPUT data_edit;
		ELSE delete;
	END;
RUN;

/* ƥ�������ҵ���� */
PROC SQL;
	CREATE TABLE data_raw AS
	SELECT A.stock_code, A.stock_name, B.indus_code, B.indus_name,
	1 AS indus_wt, "&fund_name." AS fund_name LENGTH 32,
	dhms("31dec2014"d, 0,0,0) AS into_time FORMAT datetime20.
	FROM data_edit A LEFT JOIN product.taobao_indus_code B
	ON A.fg_level2_name = B.indus_name
	ORDER BY A.stock_code;
QUIT;


/** �����ͳһ�ĸ�ʽ */
DATA &output_table.(drop = stock_code2 stock_name2);
	SET data_raw(rename = (stock_code = stock_code2 stock_name = stock_name2));
	LENGTH stock_code $ 32;
	LENGTH stock_name $ 32;
	stock_code = stock_code2;
	stock_name = stock_name2;
RUN;
	PROC SQL;
		DROP TABLE data_raw,data_edit;
	QUIT;
%MEND gen_stock_mapping;

%gen_stock_mapping(medicine_ed4.xlsx, medicine_map, TAOBAO_MEDICINE);
%gen_stock_mapping(entertainment_ed3.xlsx, entertain_map, TAOBAO_ENTERTAIN);
DATA entertain_map;
	SET entertain_map;
	IF indus_name = "��Ʊ" THEN delete;
RUN;

/** ���ɷݹ�������ÿ�µף����޳�������Ҫ��ĳɷݹ� */
/* ��ĩ���� */
PROC SQL;
	CREATE TABLE month_busdate AS
	SELECT date AS end_date LABEL "end_date"
	FROM busday
	GROUP BY year(date), month(date)
	HAVING date = max(date);
QUIT;

%MACRO gen_mapping_extend(input_table, start_date, end_date, output_table);
	/** �ÿ����ĩ */
	PROC SQL;
		CREATE TABLE tt_data AS
		SELECT A.end_date, B.*
		FROM month_busdate A, &input_table. B
		WHERE "&start_date."d <= A.end_date <= "&end_date."d
		ORDER BY A.end_date, B.stock_code;
	QUIT;

	/** ���������ݽ���ƥ�䣬���û���������ݣ����ʾ��δ���У��������У�����ͣ���У��޳���Щ�ɷֹ� */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.close, B.factor, C.freeshare
		FROM tt_data A LEFT JOIN hqinfo B
		ON A.end_date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN fg_wind_freeshare C
		ON A.end_date = datepart(C.end_date) AND A.stock_code = C.stock_code
		ORDER BY A.end_date, B.stock_code;
	QUIT;

	DATA &output_table.(drop = close factor freeshare);
		SET tmp;
		IF missing(close) OR missing(factor) OR close = 0 THEN delete;
		IF missing(freeshare) OR freeshare = 0 THEN delete;
	RUN;
%MEND gen_mapping_extend;

%gen_mapping_extend(input_table=entertain_map, start_date=15jan2011, 
		end_date=31dec2014, output_table=entertain_map2);
%gen_mapping_extend(input_table=medicine_map, start_date=15dec2008, 
		end_date=31dec2014, output_table=medicine_map2);


/** ���������в��ֹ�Ʊ����Ϊ������Ҫ�޳� */
DATA entertain_map2;
	SET entertain_map2;
	/** �����쳣��Ʊ */
	IF stock_code = "000681" AND end_date <= "21aug2013"d THEN delete ;  /* �Ӿ��й� */
	IF stock_code = "000156" AND end_date <= "19oct2012"d THEN delete; /* ������ý */
	IF stock_code = "600633" AND end_date <= "29sep2011"d THEN delete; /* �㱨��ý */
RUN;

	
/** ����һ�ű��� */
DATA product.taobao_mapping;
	SET medicine_map2 entertain_map2;
RUN;

/******************* ģ��2: ����������׼����������� ************/
/** 1- ҽҩ��ҵ�����ɵ÷� **/
/** �Ը���������Ϊ��Ʊ�� */
/** �õ��ֳɵı��: fg_csi800_factor �� taobao.med_taobao_score2 **/
PROC SQL;
	CREATE TABLE med_score AS
	SELECT datepart(A.end_date) AS end_date FORMAT yymmdd10., A.stock_code, 
		A.tot AS fg_score LABEL "fg_score" ,
		B.taobao AS taobao_score LABEL "taobao_score"
	FROM taobao.fg_csi800_factor A LEFT JOIN taobao.med_taobao_score2 B
	ON datepart(A.end_date) = B.end_date AND A.stock_code = B.stock_code
	WHERE A.stock_code NOT IN ("600851", "002614", "600490", "300061")
	AND "15dec2008"d <= datepart(A.end_date) <= "31dec2014"d
	ORDER BY A.end_date, A.stock_code;
QUIT;
/** ���ɹ�Ʊ��� */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.stock_name
	FROM med_score A LEFT JOIN taobao.stock_info_table B
	ON A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;

/** ���������ݽ���ƥ�䣬���û���������ݣ����ʾ��δ���У��������У�����ͣ���У��޳���Щ�ɷֹ� */
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, B.close, B.factor, C.freeshare
	FROM tmp A LEFT JOIN hqinfo B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code 
	LEFT JOIN fg_wind_freeshare C
	ON A.stock_code = C.stock_code AND A.end_date = datepart(C.end_date)
	ORDER BY A.end_date, B.stock_code;
QUIT;


/** �����ͳһ�ĸ�ʽ */
DATA med_score(drop = stock_code2 stock_name2 close factor freeshare);
	SET tmp2(rename = (stock_code = stock_code2 stock_name = stock_name2));
	IF missing(close) OR missing(factor) OR close = 0 THEN delete;
	IF missing(freeshare) OR freeshare = 0 THEN delete;
	LENGTH stock_code $ 32;
	LENGTH stock_name $ 32;
	stock_code = stock_code2;
	stock_name = stock_name2;
	into_time = dhms("31dec2014"d, 0,0,0);
	FORMAT into_time datetime20.;
	LENGTH fund_name $ 32;
	fund_name = "TAOBAO_MEDICINE";
	IF missing(taobao_score) THEN taobao_score = 0;
	IF missing(fg_score) THEN fg_score = 0;
RUN;



/** 2-ҽҩ��ҵ����ҵ�÷� */
PROC IMPORT OUT = factors_value
            DATAFILE= "&input_dir.\medicine_taobao_factor.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="��ҵ�¶ȵ÷�$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
PROC TRANSPOSE DATA = factors_value OUT = factors_value2(drop = _LABEL_ rename=(_NAME_ = fg_level2_name col1 = taobao));
	VAR ������ʹ--����Ѫ��;
	BY end_date;
RUN;

/** ��ԭʼ�����е��·�(Ĭ��Ϊÿ����1��)����Ϊÿ�������һ�������� */
PROC SQL;
	CREATE TABLE factors_value AS
	SELECT A.fg_level2_name, A.taobao AS taobao_score, B.end_date 
	FROM factors_value2 A LEFT JOIN 
	(
	SELECT date AS end_date LABEL "end_date"
	FROM busday
	GROUP BY year(date), month(date)
	HAVING date = max(date)
	) B
	ON year(A.end_date) = year(B.end_date) AND month(A.end_date) = month(B.end_date)
	ORDER BY B.end_date, A.fg_level2_name;
QUIT;
/** ��ȡ��ҵ���� */
PROC SQL;
	CREATE TABLE med_score_i AS
	SELECT A.end_date, B.indus_code, B.indus_name, 
	A.taobao_score, . AS fg_score,
	B.fund_name, 
	dhms("31dec2014"d, 0,0,0) AS into_time FORMAT datetime20.
	FROM factors_value A LEFT JOIN product.taobao_indus_code B
	ON A.fg_level2_name = B.indus_name
	AND fund_name = "TAOBAO_MEDICINE"
	ORDER BY A.end_date, B.indus_code;
QUIT;
DATA med_score_i;
	SET med_score_i;
	IF missing(taobao_score) THEN taobao_score = 0;
RUN;

/** 3- ���������ҵ�÷� */
/** �������ӵ÷� */
/*PROC IMPORT OUT = entertain_f */
/*            DATAFILE= "&input_dir.\factors_value_ed6.xlsx" */
/*            DBMS=EXCEL REPLACE;*/
/*     RANGE="��ҵ�¶ȵ÷�$"; */
/*     GETNAMES=YES;*/
/*     MIXED=NO;*/
/*     SCANTEXT=NO;*/
/*     USEDATE=YES;*/
/*     SCANTIME=YES;*/
/*RUN;*/

/** Step1-appendix: �����Ա��������� */
/*PROC IMPORT OUT = entertain_t*/
/*            DATAFILE= "&input_dir.\���������Ա�����20150323.xlsx" */
/*            DBMS=EXCEL REPLACE;*/
/*     RANGE="��ҵ�¶ȵ÷�$"; */
/*     GETNAMES=YES;*/
/*     MIXED=NO;*/
/*     SCANTEXT=NO;*/
/*     USEDATE=YES;*/
/*     SCANTIME=YES;*/
/*RUN;*/
/*PROC TRANSPOSE DATA = entertain_t OUT = entertain_t(drop = _LABEL_ rename=(_NAME_ = fg_level2_name col1 = taobao));*/
/*	VAR Ӱ�Ӷ���--������Ƶ;*/
/*	BY end_date;*/
/*RUN;*/
%INCLUDE "D:\Research\������\����-��Ʒ\sascode\�Ӱ��������ȡ�Ա�����.sas";
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.end_date, A.fg_level2_name, 
	A.tot2 AS fg_score LABEL "fg_score",  /* ѡ��tot2 */
	B.taobao AS taobao_score LABEL "taobao_score"
	FROM entertain_f A LEFT JOIN entertain_t B
	ON A.end_date = B.end_date AND A.fg_level2_name = B.fg_level2_name
	WHERE  "15jan2011"d <= A.end_date <= "31dec2014"d
	ORDER BY A.end_date, A.fg_level2_name;
QUIT;
DATA factors_value;
	SET tmp;
	IF fg_level2_name = "����" THEN delete; /* �޳��������ա� */
RUN;

/** ��ȡ��ҵ���� */
PROC SQL;
	CREATE TABLE entertain_score_i AS
	SELECT A.end_date, B.indus_code, B.indus_name, 
	A.taobao_score, A.fg_score,
	B.fund_name, 
	dhms("31dec2014"d, 0,0,0) AS into_time FORMAT datetime20.
	FROM factors_value A LEFT JOIN product.taobao_indus_code B
	ON A.fg_level2_name = B.indus_name
	AND fund_name = "TAOBAO_ENTERTAIN"
	ORDER BY A.end_date, B.indus_code;
QUIT;
DATA entertain_score_i;
	SET entertain_score_i;
	IF missing(taobao_score) THEN taobao_score = 0;
	IF missing(fg_score) THEN fg_score = 0;
RUN;

/* 4- ����������ɸ����÷֣��Դ���Ϊ��Ʊ�أ� */
PROC SQL;
	CREATE TABLE entertain_score AS
	SELECT A.end_date, A.stock_code, A.stock_name,
	B.taobao_score, B.fg_score,
	A.fund_name,
	dhms("31dec2014"d, 0,0,0) AS into_time FORMAT datetime20.
	FROM product.taobao_mapping A LEFT JOIN entertain_score_i B
	ON A.end_date= B.end_date AND A.indus_code = B.indus_code
	WHERE B.fund_name = "TAOBAO_ENTERTAIN"
	ORDER BY A.end_date, A.stock_code;
QUIT;

/* 5- �ϲ�����ָ�������� */
DATA product.taobao_score_i;
	SET med_score_i entertain_score_i;
RUN;
DATA product.taobao_score;
	SET med_score entertain_score;
RUN;

/******************* ģ��3: ȷ���ۺ����ӵ÷� ************/
%MACRO gen_tot(input_table, taobao_pct, output_table, fund_name);
	DATA &output_table.;
		SET &input_table.;
		IF fund_name = "&fund_name." THEN DO;
			tot_score = (1-&taobao_pct.) * fg_score + &taobao_pct. * taobao_score;
			taobao_pct = &taobao_pct.;
		END;
	RUN;
%MEND gen_tot;
/** ҽҩ���Ա�Ȩ��0.2 */
%gen_tot(input_table=product.taobao_score, taobao_pct=0.2, 
	output_table=product.taobao_score,fund_name = TAOBAO_MEDICINE);

/** ���֣��Ա�Ȩ��0.5 */
%gen_tot(input_table=product.taobao_score, taobao_pct=0.5, 
	output_table=product.taobao_score,fund_name = TAOBAO_ENTERTAIN);
%gen_tot(input_table=product.taobao_score_i, taobao_pct=0.5, 
	output_table=product.taobao_score_i,fund_name = TAOBAO_ENTERTAIN);


/******************* ģ��4-1: ҽҩָ���ӱ�ѡ����ɸѡ���ɷݹ� ************/
%LET threshold = 50;
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
	IF "15dec2008"d <= end_date <= "31dec2014"d;
RUN;



DATA sub_test(rename = (end_date = date));
	SET product.taobao_score;
	weight = 1;
	IF fund_name = "TAOBAO_MEDICINE";
RUN;
PROC SORT DATA = sub_test;
	BY date stock_code;
RUN;

/** threshold: �����������Ⱥ� **/
PROC SORT DATA = sub_test;
	BY date descending tot_score descending fg_score;
RUN;

/** ������������ע����*/
DATA score_rank(keep = date stock_code tot_score);
	SET sub_test;
	BY date;
	RETAIN rank_order 0;
	IF first.date THEN rank_order = 0;
	rank_order + 1;
	IF rank_order <= &threshold. AND tot_score > 0 THEN tot_score= 1;
	ELSE tot_score  = 0;
RUN;


%MACRO single_factor_improve_neat(fname, mark_table, output_table, is_equal);
	/* ȷ���ɷֹɷ��� */
	/** ���test_stock_pool��Ϊ������ϣ�����һ����������Ч������date�����ӵ�end_dateǡ�ö�Ӧ */
	PROC SQL;
		CREATE TABLE test_stock_pool AS
		SELECT A.*, B.&fname AS &fname._score
		FROM sub_test A LEFT JOIN &mark_table. B
		ON A.date = B.date AND A.stock_code = B.stock_code
		ORDER BY A.date, A.stock_code;
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
		ELSE weight = 0;  /* һ������²���������״�� */
	RUN;

	%gen_adjust_pool(stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, move_date_forward=0, output_stock_pool=test_stock_pool);
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
	/** ��ҵ��ǿ */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, D.sum_score, D.sum_equal
		FROM test_stock_pool A LEFT JOIN
		(SELECT end_date, 
		sum(&fname._score * &fname.) AS sum_score, 
		sum(&fname._score) AS sum_equal 
		FROM test_stock_pool
		GROUP BY end_date) D
		ON A.end_date = D.end_date
		ORDER BY A.end_date, A.stock_code;
	QUIT;

	/** ��ǿ */
	DATA test_stock_pool(drop = adj_weight);
		SET tmp;
		IF &is_equal. = 1 THEN DO;  /* ��Ȩ */
			IF sum_equal = 0 THEN DO;  /* û��ѡ���κι�Ʊ���Ͱ��ջ�׼��(��׼����ͨ��ֵ��Ȩ) */
				adj_weight = weight;
			END;
			ELSE DO;
				IF not missing(&fname._score) THEN adj_weight = &fname._score; 	/* ȡ0����1 */  
				ELSE adj_weight = 0;
			END;
		END;
		ELSE IF &is_equal. = 0 THEN DO;  /** �÷ּ�Ȩ */
			IF sum_score = 0 THEN DO;  /* û��ѡ���κι�Ʊ���Ͱ��ջ�׼�� */
				adj_weight = weight;
			END;
			ELSE DO;
				IF not missing(&fname._score) AND not missing(&fname.) THEN adj_weight = abs(&fname._score * &fname.); /* ����ֵ������ȡ���÷�*/
				ELSE adj_weight = 0;  /** ȱʧ�÷֣��÷ּ�Ȩ��ʽ�У�ֻ����Ȩ��Ϊ0 */
			END;
		END;	
		/** ����Ȩ�� */
		weight = adj_weight;
		IF weight = 0 THEN delete; /** weight = 0�Ĺ�Ʊ�����޳�*/
	RUN;
	/** ���µ�����Ȩ�أ�����Ȩ��Ϊ1 */
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
	DATA &output_table.;
		SET test_stock_pool(keep = end_date effective_date stock_code stock_name weight);
		into_time = dhms("31dec2014"d, 0,0,0);
		FORMAT into_time datetime20.;
	RUN;
%MEND single_factor_improve_neat;

%single_factor_improve_neat(fname=tot_score, mark_table=score_rank, output_table=med_pool, is_equal=0);

DATA med_pool;
	SET med_pool;
	LENGTH fund_name $32. ;
	fund_name = "TAOBAO_MEDICINE";
RUN;

/******************* ģ��4-2: ����ָ���ӱ�ѡ����ɸѡ���ɷݹ� ************/
%LET improve_pct = 0.5;
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
	IF "15jan2011"d <= end_date <= "31dec2014"d;
RUN;

DATA sub_test(rename = (end_date = date));
	SET product.taobao_score;
	weight = 1;
	IF fund_name = "TAOBAO_ENTERTAIN";
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
DATA factors_value;
	SET product.taobao_score_i;
	IF fund_name = "TAOBAO_ENTERTAIN";
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
PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT A.*, B.indus_code, B.indus_name, C.&fname AS &fname._score
	FROM sub_test A LEFT JOIN product.taobao_mapping B
	ON A.date = B.end_date AND A.stock_code = B.stock_code
	LEFT JOIN &mark_table. C
	ON A.date = C.end_date AND B.indus_code = C.indus_code
	WHERE B.fund_name = "TAOBAO_ENTERTAIN"
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
	SELECT A.stock_code, A.stock_name, A.end_date, A.effective_date, A.weight AS base_wt, B.weight AS im_wt
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
	into_time = dhms("31dec2014"d, 0,0,0);
	FORMAT into_time datetime20.;
RUN;
PROC SORT DATA = entertain_pool;
	BY end_date descending weight;
RUN;
DATA product.taobao_stock_pool;
	SET entertain_pool med_pool;
RUN;


/********** ����ģ�飺�����֮ǰ��ָ����˾�İ汾�Ƿ���ͬ ****/
/** 1- �Ա��������� */
/*** ��ȫһ�� **/
PROC IMPORT OUT = zs_entertain
            DATAFILE= "D:\Research\������\�ṩ��ָ����˾\����������-��������10-update.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="�ɷݹ�$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

PROC SQL;
	CREATE TABLE ent_cmp AS
	SELECT A.end_date AS end_date_a, A.stock_code AS stock_code_a, A.weight AS weight_a,
	B.end_date AS end_date_b, B.stock_code AS stock_code_b, B.weight AS weight_b
	FROM entertain_pool A FULL JOIN zs_entertain B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, B.end_date, A.stock_code, B.stock_code;
QUIT;
DATA ent_cmp;
	SET ent_cmp;
	delta_w = abs(weight_a - weight_b);
	IF delta_w >= 0.0001 OR missing(delta_w) THEN err = 1;
	ELSE err = 0;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT *
	FROM ent_cmp
	WHERE err = 1;
QUIT;

/** 2-�Ա�ҽҩָ�� **/
/**��ȷ **/
PROC IMPORT OUT = zs_med
            DATAFILE= "D:\Research\������\�ṩ��ָ����˾\�Խ�������-update.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="�ɷݹ�$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

PROC SQL;
	CREATE TABLE med_cmp AS
	SELECT A.end_date AS end_date_a, A.stock_code AS stock_code_a, A.weight AS weight_a,
	B.end_date AS end_date_b, B.stock_code AS stock_code_b, B.weight AS weight_b
	FROM med_pool A FULL JOIN zs_med B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, B.end_date, A.stock_code, B.stock_code;
QUIT;
DATA med_cmp;
	SET med_cmp;
	delta_w = abs(weight_a - weight_b);
	IF delta_w >= 0.0001 OR missing(delta_w) THEN err = 1;
	ELSE err = 0;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT *
	FROM med_cmp
	WHERE err = 1;
QUIT;
