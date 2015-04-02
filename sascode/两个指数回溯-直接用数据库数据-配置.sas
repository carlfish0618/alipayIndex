/***该版本为直接面向数据库进行回溯时，需要创建的基础表和配置　***/
/*** 基础表 ***/

%LET product_dir = F:\Research\GIT_BACKUP\alipayIndex;
%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

%INCLUDE "D:\Research\淘消费\sascode\指数构建-标准版\指数构建标准版.sas";
options validvarname=any; /* 支持中文变量名 */

%LET env_start_date = 31dec2008;

/********** 功能函数 ******************************************/
/** 1- 作用：剔除不满足条件的股票 */
/** 要求: (1) 在上市和退市日之间
(2) 未停牌 ***/

%MACRO filter_stock(input_pool, output_pool);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.vol, B.value, C.list_date, C.delist_date
		FROM &input_pool. A LEFT JOIN hqinfo B
		ON A.end_date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN stock_info_table C
		ON A.stock_code = C.stock_code
		ORDER BY A.end_date, B.stock_code;
	QUIT;
	DATA &output_pool.(drop = vol value list_date delist_date);
		SET tmp;
		IF vol = 0 OR value = 0 THEN delete;
		IF missing(list_date) THEN delete;
		IF not missing(list_date) AND list_date > end_date THEN delete;  /* 当天上市，调入成分股 */
		IF not missing(delist_date) AND end_date >= delist_date THEN delete;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND filter_stock;

%MACRO filter_stock_mdf(input_pool, output_pool);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.stock_code AS stock_code_b, B.sustrandays , B.lasttrandate, C.list_date, C.delist_date
		FROM &input_pool. A LEFT JOIN 
		(SELECT distinct end_Date, stock_code, sustrandays, lasttrandate
		FROM fgtest.fg_taobao_suspend) B
		ON A.end_date = datepart(B.end_date) AND A.stock_code = B.stock_code 
		LEFT JOIN stock_info_table C
		ON A.stock_code = C.stock_code
		ORDER BY A.end_date, B.stock_code;
	QUIT;
	DATA &output_pool.(drop = stock_code_b sustrandays lasttrandate list_date delist_date);
		SET tmp;
		IF not missing(stock_code_b) AND sustrandays >= 5 THEN delete;
		IF not missing(stock_code_b) AND (missing(lasttrandate) OR lasttrandate = .) THEN delete;
		IF missing(list_date) THEN delete;
		IF not missing(list_date) AND list_date > end_date THEN delete;  /* 当天上市，调入成分股 */
		IF not missing(delist_date) AND end_date >= delist_date THEN delete;
	RUN;
/*	PROC SQL;*/
/*		DROP TABLE tmp;*/
/*	QUIT;*/
%MEND filter_stock_mdf;



/*** 2- 从外部导入匹配 **/
%MACRO gen_stock_mapping(input_file, output_table, fund_name);
	PROC IMPORT OUT = data_raw
            DATAFILE= "&input_dir.\&input_file." 
            DBMS=EXCEL REPLACE;
     	RANGE="股票和主题代码匹配$"; 
     	GETNAMES=YES;
     	MIXED=NO;
     	SCANTEXT=NO;
     	USEDATE=YES;
     	SCANTIME=YES;
	RUN;


/* 匹配二级行业代码 */
PROC SQL;
	CREATE TABLE data_edit AS
	SELECT A.*,
		"&fund_name." AS fund_name LENGTH 32
	FROM data_raw A LEFT JOIN fgtest.fg_taobao_indus B
	ON A.indus_name = B.indus_name
	ORDER BY A.stock_code;
QUIT;


/** 处理成统一的格式 */
DATA &output_table.(drop = stock_code2);
	SET data_edit(rename = (stock_code = stock_code2));
	LENGTH stock_code $ 32;
	stock_code = stock_code2;
RUN;
	PROC SQL;
		DROP TABLE data_raw, data_edit;
	QUIT;
%MEND gen_stock_mapping;

/*** 3- 把单期匹配扩展到历史期间 */
%MACRO gen_mapping_extend(adjust_table, input_table, start_date, end_date, output_table, is_filter =1);
	/** 填到每个月末 */
	PROC SQL;
		CREATE TABLE tt_data AS
		SELECT A.end_date, B.*
		FROM &adjust_table. A, &input_table. B
		WHERE "&start_date."d <= A.end_date <= "&end_date."d
		ORDER BY A.end_date, B.stock_code;
	QUIT;
	%IF %SYSEVALF(&is_filter. = 1) %THEN %DO;
		%filter_stock(input_pool=tt_data, output_pool=&output_table.);
	%END;
	%ELSE %DO;
		DATA &output_table.;
			SET tt_data;
		RUN;
	%END;
		
	PROC SQL;
		DROP TABLE tt_data;
	QUIT;

%MEND gen_mapping_extend;


/**** 4- 从行业代码，匹配行业名称（或反之) **/
%MACRO get_indus_name(input_table, output_table, fund_name, is_reverse = 0);
	%IF %SYSEVALF(&is_reverse. = 0) %THEN %DO;
		%LET from_name = indus_code;
		%LET to_name = indus_name;
	%END;
	%ELSE %DO;
		%LET from_name = indus_name;
		%LET to_name = indus_code;
	%END;

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.&to_name.
		FROM &input_table. A LEFT JOIN fgtest.fg_taobao_indus B
		ON A.&from_name. = B.&from_name.
		AND B.fund_name = "&fund_name.";
	QUIT;
	DATA &output_table.;
		SET tmp;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND get_indus_name;



/** 5- 计算有匹配的淘宝个股得分 */
/** 注：如果某个行业缺失行业得分，则在计算加权淘宝得分时，该行业认定为无效（即权重不计入加权中) **/
/** 个股-行业匹配来自: fgtest.fg_taobao_map */
/** 行业得分来自: fgtest.fg_taobao_rawdata */


%MACRO gen_taobao_score(mapping_table, score_table, fund_name,  output_table);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.end_date, A.stock_code, A.indus_code, A.indus_weight, B.data_value
		FROM &mapping_table. A 
		LEFT JOIN &score_table. B
		ON A.indus_code = B.indus_code AND A.end_date = B.end_date
		AND A.fund_name = "&fund_name."   /* 这里不能使用where。因为这表示连接后再筛选 */
		AND B.data_type = "score"
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.end_date, A.stock_code, A.indus_count, A.indus_sum/B.t_weight AS taobao_score
		FROM 
		(SELECT end_date, stock_code, sum(indus_weight*data_value) AS indus_sum, count(distinct indus_code) AS indus_count
		 FROM tmp 
		GROUP BY end_date, stock_code) A 
		LEFT JOIN
		(SELECT end_date, stock_code, sum(indus_weight) AS t_weight
		FROM tmp
/*		WHERE not missing(data_value)*/
		GROUP BY end_date, stock_code) B
		ON A.end_date = B.end_date AND A.stock_code = B.stock_code
		ORDER BY end_date, stock_code;
	QUIT;
	DATA &output_table.;
		SET tmp2;
	RUN;
	PROC SQL;
		DROP TABLE tmp2;
	QUIT;
%MEND gen_taobao_score;

/*** 6- 单因子标准化 **/

%MACRO cal_percentitle(input_table, colname, pct, output_table);
	PROC SORT DATA = &input_table.;
		BY end_date &colname.;
	RUN;
	/** Step1: 计算每天非空的样本数 */
	PROC SQL;
		CREATE TABLE tt_intval AS
		SELECT end_date, count(1) AS nobs
		FROM &input_table.
		WHERE not missing(&colname.)
		GROUP BY end_date;
	QUIT;
	DATA tt_intval;
		SET tt_intval;
		nn = &pct./100 * (nobs-1)+1;
		nk = floor(nn);
		nd = nn - nk;
	RUN;
	/** Step2: 给原始数据排序 */
	DATA tt_nomissing;
		SET &input_table.(keep = end_date stock_code &colname.);
		IF not missing(&colname.);
	RUN;
	PROC SORT DATA = tt_nomissing;
		BY end_Date &colname.;
	RUN;
	DATA tt_nomissing;
		SET tt_nomissing;
		BY end_Date;
		RETAIN rank 0;
		IF first.end_date THEN rank = 0;
		rank + 1;
	RUN;
	PROC SQL;
		CREATE TABLE tt_intval2 AS
		SELECT A.*, B.&colname. AS v_1, C.&colname. AS v_n, D.&colname. AS v_k, E.&colname. AS v_k1
		FROM tt_intval A LEFT JOIN tt_nomissing B
		ON A.end_date = B.end_date AND B.rank = 1
		LEFT JOIN tt_nomissing C
		ON A.end_Date = C.end_date AND C.rank = A.nobs
		LEFT JOIN tt_nomissing D
		ON A.end_Date = D.end_Date AND D.rank = A.nk
		LEFT JOIN tt_nomissing E
		ON A.end_date = E.end_date AND E.rank = A.nk+1;
	QUIT;
	DATA tt_intval(keep = end_date pct&pct.);
		SET tt_intval2;
		IF nk = 0 THEN pct&pct. = v_1;
		ELSE IF nk = nobs THEN pct&pct. = v_n;
		ELSE pct&pct. = v_k + nd*(v_k1-v_k);
	RUN;
	DATA &output_table.;
		SET tt_intval;
	RUN;
	PROC SQL;
		DROP TABLE tt_intval, tt_intval2, tt_nomissing;
	QUIT;

%MEND cal_percentitle;






%MACRO normalize_single_score(input_table, colname, output_table, is_replace = 1);
	/** 分位数的计算需要重新写一个函数 */
	/** 参考wikipedia中Excel的方法 */
/*	PROC UNIVARIATE DATA = &input_table NOPRINT PCTLDEF=5;*/
/*		VAR &colname.;*/
/*		WHERE not missing(&colname.);*/
/*		BY end_date;*/
/*		OUTPUT OUT = tmp pctlpts = 1 99 pctlpre = pct;*/
/*	QUIT;*/
	%cal_percentitle(&input_table., &colname., 1, pct1);
	%cal_percentitle(&input_table., &colname., 99, pct99);
	PROC SQL;
		CREATE TABLE tt_raw AS
		SELECT A.end_date, A.stock_code, A.&colname., A.fmv_sqr, B.pct1, C.pct99
		FROM &input_table. A LEFT JOIN pct1 B
		ON A.end_date = B.end_date
		LEFT JOIN pct99 C
		ON A.end_Date = C.end_date
		ORDER BY A.end_date;
	QUIT;
	/* winsorize */
	DATA tt_raw;
		SET tt_raw;
		IF not missing(&colname.) AND &colname. > pct99 THEN &colname._mdf = pct99;
		ELSE IF not missing(&colname.) AND &colname. < pct1 THEN &colname._mdf = pct1;
		ELSE IF not missing(&colname.) THEN &colname._mdf = &colname.;
		ELSE &colname._mdf = .;
	RUN;
	/* normalized */
	PROC SQL;
		CREATE TABLE tmp_std AS
		SELECT end_date, std(&colname._mdf) AS std, sum(&colname._mdf * fmv_sqr) AS sum_wt
		FROM tt_raw
		GROUP BY end_date;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp_fmv AS
		SELECT end_date, sum(fmv_sqr) AS sum_fmv
		FROM tt_raw
		WHERE not missing(&colname._mdf) AND not missing(fmv_sqr)
		GROUP BY end_Date;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp_normal AS
		SELECT A.end_date, A.stock_code, A.&colname._mdf, B.std, B.sum_wt, C.sum_fmv
		FROM tt_raw A LEFT JOIN tmp_std B
		ON A.end_date = B.end_date
		LEFT JOIN tmp_fmv C
		ON A.end_date = C.end_date
		ORDER BY A.end_date, A.stock_code;
	QUIT;

	DATA tmp_normal(drop = std sum_wt sum_fmv);
		SET tmp_normal;
		IF not missing(&colname._mdf) THEN &colname._mdf = (&colname._mdf - sum_wt/sum_fmv)/std;
	RUN;
	%IF %SYSEVALF(&is_replace.) %THEN %DO;   /* 替换初始值 */
		DATA tmp_normal(drop = &colname._mdf);
			SET tmp_normal;
			&colname = &colname._mdf;
		RUN;
	%END;
	PROC SORT DATA = &input_table.;
		BY end_date stock_code;
	RUN;
	PROC SORT DATA = tmp_normal;
		BY end_date stock_code;
	RUN;
	DATA &output_table.;
		UPDATE &input_table. tmp_normal;
		BY end_date stock_code;
	RUN;
	PROC SQL;
		DROP TABLE tt_raw, tmp_std, tmp_fmv, tmp_normal, pct1, pct99;
	QUIT;
%MEND  normalize_single_score;

/** exclude_list范例：(TOT) **/
%MACRO 	normalize_multi_score(input_table, output_table, exclude_list);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist;
		SET tt_varlist;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list SEPARATED BY ' ',
               :nfactors
          FROM tt_varlist;
     QUIT;
              
     %DO i = 1 %TO &nfactors.;
          %LET fname =  %scan(&name_list.,&i., ' ');
          %normalize_single_score(rr_result, &fname., rr_result);
     %END;
	 DATA &output_table.;
	 	SET rr_result;
	RUN;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist;
	QUIT;
%MEND normalize_multi_score;


/** 8- 计算因子加权权重 **/
/** 目前仅支持，因子权重就一个版本的（即只有一个end_date) */
%MACRO single_factor_score(input_table, colname, output_table, is_replace=1, fsector = FG4);
	
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.end_date, A.stock_code, A.&colname., B.weight
		FROM &input_table. A LEFT JOIN fgtest.fg_score_weight B
		ON upcase(B.fsignal) = upcase("&colname.")
		WHERE fsector = "&fsector."
		ORDER BY A.end_date;
	QUIT;
	DATA tmp(drop = weight);
		SET tmp;
		IF not missing(weight) THEN &colname._mdf = weight * &colname.;
		ELSE &colname._mdf = 0;
	RUN;
	%IF %SYSEVALF(&is_replace.=1) %THEN %DO;   /* 替换初始值 */
		DATA tmp(drop = &colname._mdf);
			SET tmp;
			&colname. = &colname._mdf;
		RUN;
	%END;
	PROC SORT DATA = &input_table.;
		BY end_date stock_code;
	RUN;
	PROC SORT DATA = tmp;
		BY end_date stock_code;
	RUN;
	DATA &output_table.;
		UPDATE &input_table. tmp;
		BY end_date stock_code;
	RUN;
/*	PROC SQL;*/
/*		DROP TABLE tmp;*/
/*	QUIT;*/
%MEND single_factor_score;


/** exclude_list范例：(TOT) **/
%MACRO multi_factor_score(input_table, output_table, exclude_list);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist;
		SET tt_varlist;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list SEPARATED BY ' ',
               :nfactors
          FROM tt_varlist;
     QUIT;
              
     %DO i = 1 %TO &nfactors.;
          %LET fname =  %scan(&name_list.,&i., ' ');
          %single_factor_score(rr_result, &fname., rr_result);
     %END;
	 DATA &output_table.;
	 	SET rr_result;
	RUN;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist;
	QUIT;
%MEND multi_factor_score;
	

/** 9- 计算综合得分 */
%MACRO cal_tot(input_table, v_tbpct, output_table);
	DATA &output_table.(drop = taobao_score_mdf fg_score_mdf);
		SET &input_table.;
		IF missing(fg_score) THEN fg_score_mdf = 0;
		ELSE fg_score_mdf = fg_score;
		IF missing(taobao_score) THEN taobao_score_mdf = 0;
		ELSE taobao_score_mdf = taobao_score;
		tot_score = (1-&v_tbpct.) * fg_score_mdf + &v_tbpct.* taobao_score_mdf;
	RUN;
%MEND cal_tot;


/*********************** 准备基础数据表到本地 *******************/

/*** (1) 生成日期列表 */
PROC SQL;
	CREATE TABLE busday AS
	SELECT datepart(end_date) AS date FORMAT mmddyy10. LABEL "date"
	FROM
	(
	SELECT distinct(end_date)
	FROM hq.hqinfo
	WHERE end_date >= dhms("&env_start_date."d, 0,0,0)
	)
	ORDER BY date;
QUIT;

/* (2) 行情表 */
PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT datepart(end_date) AS end_date FORMAT yymmdd10. LABEL "end_date", stock_code, close, factor, pre_close, istrade, vol, value
	FROM hq.hqinfo
	WHERE type = 'A' AND end_date >= dhms("&env_start_date."d, 0,0,0)
	ORDER BY end_date, stock_code;
QUIT;

/** (3) 自由流通市值表 */
PROC SQL;
	CREATE TABLE fg_wind_freeshare AS
	SELECT stock_code, end_date, freeshare
	FROM tinysoft.fg_wind_freeshare;
QUIT;

/*** (4) 信息表 **/ 
PROC SQL;
	CREATE TABLE stock_info_table AS
	SELECT F16_1090 AS stock_code, OB_OBJECT_NAME_1090 AS stock_name,  input(F17_1090,yymmdd8.) AS list_date FORMAT yymmdd10.,
		input(F18_1090,yymmdd8.) AS delist_date FORMAT yymmdd10., 
		F19_1090 AS is_delist, F6_1090 AS bk
	FROM locwind.tb_object_1090
	WHERE F4_1090 = 'A';
QUIT;

/*** (5) 得分表 */
/*PROC SQL;*/
/*	CREATE TABLE adjust_busdate AS*/
/*	SELECT date AS end_date LABEL "end_date"*/
/*	FROM busday*/
/*	GROUP BY year(date), month(date)*/
/*	HAVING date = max(date);*/
/*QUIT;*/
/*DATA adjust_busdate;*/
/*	SET adjust_busdate;*/
/*	IF "15dec2008"d <= end_date;*/
/*	end_date = dhms(end_date,0,0,0);*/
/*	FORMAT end_Date datetime20.;*/
/*RUN;*/
/**/
/*PROC SQL;*/
/*	CREATE TABLE fg_raw_score AS*/
/*	SELECT **/
/*	FROM score.fg_raw_score*/
/*	WHERE end_date IN (SELECT end_date FROM adjust_busdate);*/
/*QUIT;*/
/** 备份下，运行太久了 */
/*DATA product.fg_raw_score;*/
/*	SET fg_raw_score;*/
/*RUN;*/
