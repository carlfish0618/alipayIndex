/** 适合于两个指数的搭建 **/
/** 程序说明: 
(1) 第一个模块创建“手工维护”的输入表。之
(2) 第二个模块，根据现有数据创建“因子准备工作”的输出表。
    在正式运行的时候，该部分需要根据最新的输入和流程重新书写流程 ***/

%LET product_dir = D:\Research\淘消费\阿里-产品;
%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

%LET taobao_dir = D:\Research\淘消费;
LIBNAME taobao "&taobao_dir.\sasdata";
%INCLUDE "D:\Research\淘消费\sascode\指数构建-标准版\指数构建标准版.sas";
options validvarname=any; /* 支持中文变量名 */

/*********************** 准备基础数据表到本地 *******************/
%LET env_start_date = 31dec2008;

/*** (1) 生成日期列表 */
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


/* (2) 行情表 */
PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT datepart(end_date) AS end_date FORMAT yymmdd10. LABEL "end_date", stock_code, close, factor, pre_close
	FROM taobao.hqinfo
	WHERE type = 'A' AND end_date >= dhms("&env_start_date."d, 0,0,0)
	ORDER BY end_date, stock_code;
QUIT;

/** (3) 自由流通市值表 */
PROC SQL;
	CREATE TABLE fg_wind_freeshare AS
	SELECT stock_code, end_date, freeshare
	FROM taobao.fg_wind_freeshare_neat;
QUIT;


/******************* 模块1: “手工维护”输入表 ************/
/** 表1: taobao_indus_code: 行业代码和行业简称映射关系 **/

/* 导入二级行业代码和行业简称匹配 */
/** 这个版本与提供给阿里的版本完全一致。之后都以该版本为准 */
/** 医药新增: 医疗器械和医疗服务 */
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

/** 表2: taobao_mapping ***/
/** 1.两个指数成份股 */
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

/** Step1: 将多行业匹配拆分为不同的行 */
DATA data_raw data_edit(drop = fg_level2_name i rename = (fg_level2_name_update = fg_level2_name));
	SET data_raw;
	DO i = 1 TO 20;
		fg_level2_name_update = scan(fg_level2_name, i, '/');
		IF not missing(fg_level2_name_update) THEN OUTPUT data_edit;
		ELSE delete;
	END;
RUN;

/* 匹配二级行业代码 */
PROC SQL;
	CREATE TABLE data_raw AS
	SELECT A.stock_code, A.stock_name, B.indus_code, B.indus_name,
	1 AS indus_wt, "&fund_name." AS fund_name LENGTH 32,
	dhms("31dec2014"d, 0,0,0) AS into_time FORMAT datetime20.
	FROM data_edit A LEFT JOIN product.taobao_indus_code B
	ON A.fg_level2_name = B.indus_name
	ORDER BY A.stock_code;
QUIT;


/** 处理成统一的格式 */
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
	IF indus_name = "机票" THEN delete;
RUN;

/** 将成份股扩充至每月底，并剔除不符合要求的成份股 */
/* 月末数据 */
PROC SQL;
	CREATE TABLE month_busdate AS
	SELECT date AS end_date LABEL "end_date"
	FROM busday
	GROUP BY year(date), month(date)
	HAVING date = max(date);
QUIT;

%MACRO gen_mapping_extend(input_table, start_date, end_date, output_table);
	/** 填到每个月末 */
	PROC SQL;
		CREATE TABLE tt_data AS
		SELECT A.end_date, B.*
		FROM month_busdate A, &input_table. B
		WHERE "&start_date."d <= A.end_date <= "&end_date."d
		ORDER BY A.end_date, B.stock_code;
	QUIT;

	/** 与行情数据进行匹配，如果没有行情数据，则表示尚未上市，或已退市，或暂停上市，剔除这些成分股 */
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


/** 娱乐生活有部分股票，因为重组需要剔除 */
DATA entertain_map2;
	SET entertain_map2;
	/** 处理异常股票 */
	IF stock_code = "000681" AND end_date <= "21aug2013"d THEN delete ;  /* 视觉中国 */
	IF stock_code = "000156" AND end_date <= "19oct2012"d THEN delete; /* 华数传媒 */
	IF stock_code = "600633" AND end_date <= "29sep2011"d THEN delete; /* 浙报传媒 */
RUN;

	
/** 放入一张表中 */
DATA product.taobao_mapping;
	SET medicine_map2 entertain_map2;
RUN;

/******************* 模块2: 创建“因子准备”的输出表 ************/
/** 1- 医药行业：个股得分 **/
/** 以富国因子作为股票池 */
/** 用到现成的表格: fg_csi800_factor 和 taobao.med_taobao_score2 **/
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
/** 生成股票简称 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.stock_name
	FROM med_score A LEFT JOIN taobao.stock_info_table B
	ON A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;

/** 与行情数据进行匹配，如果没有行情数据，则表示尚未上市，或已退市，或暂停上市，剔除这些成分股 */
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, B.close, B.factor, C.freeshare
	FROM tmp A LEFT JOIN hqinfo B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code 
	LEFT JOIN fg_wind_freeshare C
	ON A.stock_code = C.stock_code AND A.end_date = datepart(C.end_date)
	ORDER BY A.end_date, B.stock_code;
QUIT;


/** 处理成统一的格式 */
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



/** 2-医药行业：行业得分 */
PROC IMPORT OUT = factors_value
            DATAFILE= "&input_dir.\medicine_taobao_factor.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="行业月度得分$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
PROC TRANSPOSE DATA = factors_value OUT = factors_value2(drop = _LABEL_ rename=(_NAME_ = fg_level2_name col1 = taobao));
	VAR 解热镇痛--心脑血管;
	BY end_date;
RUN;

/** 将原始数据中的月份(默认为每个月1日)调整为每个月最后一个交易日 */
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
/** 获取行业代码 */
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

/** 3- 娱乐生活：行业得分 */
/** 富国因子得分 */
/*PROC IMPORT OUT = entertain_f */
/*            DATAFILE= "&input_dir.\factors_value_ed6.xlsx" */
/*            DBMS=EXCEL REPLACE;*/
/*     RANGE="行业月度得分$"; */
/*     GETNAMES=YES;*/
/*     MIXED=NO;*/
/*     SCANTEXT=NO;*/
/*     USEDATE=YES;*/
/*     SCANTIME=YES;*/
/*RUN;*/

/** Step1-appendix: 读入淘宝因子数据 */
/*PROC IMPORT OUT = entertain_t*/
/*            DATAFILE= "&input_dir.\娱乐生活淘宝因子20150323.xlsx" */
/*            DBMS=EXCEL REPLACE;*/
/*     RANGE="行业月度得分$"; */
/*     GETNAMES=YES;*/
/*     MIXED=NO;*/
/*     SCANTEXT=NO;*/
/*     USEDATE=YES;*/
/*     SCANTIME=YES;*/
/*RUN;*/
/*PROC TRANSPOSE DATA = entertain_t OUT = entertain_t(drop = _LABEL_ rename=(_NAME_ = fg_level2_name col1 = taobao));*/
/*	VAR 影视动漫--网络视频;*/
/*	BY end_date;*/
/*RUN;*/
%INCLUDE "D:\Research\淘消费\阿里-产品\sascode\从阿里输出获取淘宝因子.sas";
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.end_date, A.fg_level2_name, 
	A.tot2 AS fg_score LABEL "fg_score",  /* 选择tot2 */
	B.taobao AS taobao_score LABEL "taobao_score"
	FROM entertain_f A LEFT JOIN entertain_t B
	ON A.end_date = B.end_date AND A.fg_level2_name = B.fg_level2_name
	WHERE  "15jan2011"d <= A.end_date <= "31dec2014"d
	ORDER BY A.end_date, A.fg_level2_name;
QUIT;
DATA factors_value;
	SET tmp;
	IF fg_level2_name = "航空" THEN delete; /* 剔除掉“航空” */
RUN;

/** 获取行业代码 */
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

/* 4- 娱乐生活：个股富国得分（以此作为股票池） */
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

/* 5- 合并两个指数的数据 */
DATA product.taobao_score_i;
	SET med_score_i entertain_score_i;
RUN;
DATA product.taobao_score;
	SET med_score entertain_score;
RUN;

/******************* 模块3: 确定综合因子得分 ************/
%MACRO gen_tot(input_table, taobao_pct, output_table, fund_name);
	DATA &output_table.;
		SET &input_table.;
		IF fund_name = "&fund_name." THEN DO;
			tot_score = (1-&taobao_pct.) * fg_score + &taobao_pct. * taobao_score;
			taobao_pct = &taobao_pct.;
		END;
	RUN;
%MEND gen_tot;
/** 医药：淘宝权重0.2 */
%gen_tot(input_table=product.taobao_score, taobao_pct=0.2, 
	output_table=product.taobao_score,fund_name = TAOBAO_MEDICINE);

/** 娱乐：淘宝权重0.5 */
%gen_tot(input_table=product.taobao_score, taobao_pct=0.5, 
	output_table=product.taobao_score,fund_name = TAOBAO_ENTERTAIN);
%gen_tot(input_table=product.taobao_score_i, taobao_pct=0.5, 
	output_table=product.taobao_score_i,fund_name = TAOBAO_ENTERTAIN);


/******************* 模块4-1: 医药指数从备选池中筛选出成份股 ************/
%LET threshold = 50;
/** 回测日期 **/
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

/** threshold: 设置排名的先后 **/
PROC SORT DATA = sub_test;
	BY date descending tot_score descending fg_score;
RUN;

/** 创建辅助表：标注排名*/
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
	/* 确定成分股分组 */
	/** 这里：test_stock_pool作为调仓组合，从下一个交易日生效。所以date和因子的end_date恰好对应 */
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
		ELSE weight = 0;  /* 一般情况下不会出现这个状况 */
	RUN;

	%gen_adjust_pool(stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, move_date_forward=0, output_stock_pool=test_stock_pool);
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
	/** 行业增强 */
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

	/** 增强 */
	DATA test_stock_pool(drop = adj_weight);
		SET tmp;
		IF &is_equal. = 1 THEN DO;  /* 等权 */
			IF sum_equal = 0 THEN DO;  /* 没有选中任何股票，就按照基准配(基准是流通市值加权) */
				adj_weight = weight;
			END;
			ELSE DO;
				IF not missing(&fname._score) THEN adj_weight = &fname._score; 	/* 取0或者1 */  
				ELSE adj_weight = 0;
			END;
		END;
		ELSE IF &is_equal. = 0 THEN DO;  /** 得分加权 */
			IF sum_score = 0 THEN DO;  /* 没有选中任何股票，就按照基准配 */
				adj_weight = weight;
			END;
			ELSE DO;
				IF not missing(&fname._score) AND not missing(&fname.) THEN adj_weight = abs(&fname._score * &fname.); /* 绝对值，允许取负得分*/
				ELSE adj_weight = 0;  /** 缺失得分，得分加权方式中，只能令权重为0 */
			END;
		END;	
		/** 更新权重 */
		weight = adj_weight;
		IF weight = 0 THEN delete; /** weight = 0的股票予以剔除*/
	RUN;
	/** 重新调整下权重，让总权重为1 */
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

/******************* 模块4-2: 娱乐指数从备选池中筛选出成份股 ************/
%LET improve_pct = 0.5;
/** 回测日期 **/
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


/** 1- 基准部分 **/
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


/** 2. 增强部分 **/
%LET mark_table = score_half;
%LET is_equal =0;
%LET fname = tot_score;

/** 创建辅助表：标注得分是否大于位于前50%行业 */
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

/** 取行业信息 */
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
	/** 行业增强 */
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
		mean(&fname._score * &fname.) AS indus_score, /* 得分加权 */
		mean(&fname._score) AS indus_equal /* 等权 */
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

	/** 增强 */
	DATA test_stock_pool;
		SET tmp;
		IF &is_equal. = 1 THEN DO;  /* 等权 */
			IF sum_indus_equal = 0 THEN DO;  /* 没有选中任何行业，就按照基准配 */
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
			IF sum_indus_score = 0 THEN DO;  /* 没有选中任何行业，就按照基准配 */
				adj_indus_wt = indus_wt;
				multiplier = 1;
				adj_weight = weight * multiplier;
			END;
			ELSE DO;
				adj_indus_wt = indus_score; /* 得分加权 */
				multiplier = adj_indus_wt / indus_wt;
				adj_weight = weight * multiplier;
			END;
		END;
		/** 更新权重 */
		weight = adj_weight;
	RUN;
	/** 重新调整下权重，让总权重为1 */
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
DATA im_stock_pool;
	SET test_stock_pool;
RUN;
PROC SORT DATA = im_stock_pool;
	BY end_date;
RUN;


/** 基准+增强 */

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

/** 避免极端情况，所有股票的im_wt都为0 **/
/** 其实这个步骤也可以不用。因为在增强部分设定了，如果没有选中行业则还按照基准配置。**/
%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);

/* 限制个股权重 */
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


/********** 补充模块：检验和之前给指数公司的版本是否相同 ****/
/** 1- 对比娱乐生活 */
/*** 完全一致 **/
PROC IMPORT OUT = zs_entertain
            DATAFILE= "D:\Research\淘消费\提供给指数公司\淘娱乐生活-个股上限10-update.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="成份股$"; 
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

/** 2-对比医药指数 **/
/**正确 **/
PROC IMPORT OUT = zs_med
            DATAFILE= "D:\Research\淘消费\提供给指数公司\淘健康生活-update.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="成份股$"; 
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
