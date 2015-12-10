/** ======================说明=================================================**/
/*** 函数功能：提供与日期无关的辅助的通用函数 **** /

/**** 函数列表:
(1) get_sector_info: 提取行业信息
(2) get_stock_size: 提取市值、流通市值信息等
(3) read_from_excel: 从Excel中读取文件
(4) output_to_excel: 输出文件到Excel中
(5) plot_normal: 画正态图
(6) cal_dist:计算分布情况
(7) mark_in_table: 判断不同股票组合之间的重叠度
(8) gen_macro_var_list: 生成宏变量
(9) get_nearby_data: 取间隔N期的数据
(10) output_to_csv: 输出到csv

****/ 

/** =======================================================================**/


options validvarname=any; /* 支持中文变量名 */

/** 模块1: 提取行业信息 */
/** 输入:
(1) stock_table: stock_code/end_date/其他
(2) mapping_table: stock_code/end_date/indus_code/indus_name  */

/** 输出:
(1) output_stock_table: end_date/stock_code/indus_code/indus_name/其他  **/

%MACRO get_sector_info(stock_table, mapping_table, output_stock_table);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.indus_code, B.indus_name
		FROM &stock_table. A LEFT JOIN &mapping_table. B
		ON A.stock_code = B.stock_code AND A.end_date > B.end_date  AND A.end_date <= B.end_date + 100 /* 最多取100天就够了 */
		GROUP BY A.end_date, A.stock_code
		HAVING B.end_date = max(B.end_date) 
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	/** 如果行业信息缺失，有可能是新股，尚未更新行业信息。这时候用已发布的，第一条记录的行业补充 */
	PROC SQL;
		CREATE TABLE miss_subset AS
		SELECT stock_code, end_date
		FROM tmp 
		WHERE missing(indus_code);
	QUIT;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.indus_code, B.indus_name
		FROM miss_subset A LEFT JOIN &mapping_table. B
		ON A.stock_code = B.stock_code AND B.end_date >= A.end_date
		GROUP BY A.stock_code, A.end_date
		HAVING B.end_date = min(B.end_date)
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	/** 更新 */
	DATA tmp;
		UPDATE tmp tmp2;
		BY end_date stock_code;
	RUN;

	DATA &output_stock_table.;
		SET tmp;
	RUN;
	PROC SQL;
		DROP TABLE tmp, tmp2, miss_subset;
	QUIT;

%MEND get_sector_info;


/** 模块2: 提取个股信息 */
/** 输入:
(1) stock_table: stock_code/end_date/其他
(2) index(numeric): 可以取 1-freeshare 2-a_share 3-total_share 4-liqa_share中一者。
(3) info_table: stock_code/end_date/close
(4) share_table: stock_code/end_date/freeshare(a_share,total_share,liqa_share等) 
(5) colname: 输出的市值列进行重命名

/** 输出:
(1) output_stock_table: end_date/stock_code/value  **/
%MACRO get_stock_size(stock_table, info_table, share_table,output_table, colname, index = 1);
	%IF %SYSEVALF(&index. = 1) %THEN %DO;
		%LET var_name = freeshare;
	%END;
	%ELSE %IF %SYSEVALF(&index. = 2) %THEN %DO;
		%LET var_name = a_share;
	%END;
	%ELSE %IF %SYSEVALF(&index. = 3) %THEN %DO;
		%LET var_name = total_share;
	%END;
	%ELSE %IF %SYSEVALF(&index. = 4) %THEN %DO;
		%LET var_name = liqa_share;
	%END;
	

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.close, C.&var_name.
		FROM &stock_table. A LEFT JOIN &info_table. B
		ON A.stock_code = B.stock_code AND A.end_Date = B.end_date
		LEFT JOIN &share_table. C
		ON A.stock_code = C.stock_code AND A.end_date = C.end_Date
		ORDER BY A.end_Date, A.stock_code;
	QUIT;
	DATA &output_table.(drop = close &var_name.);
		SET tmp;
		&colname. = close * &var_name.;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND get_stock_size;

/** 模块3： 从外部读入excel文件 */
%MACRO read_from_excel(excel_path, output_table, sheet_name = Sheet1$);
	PROC IMPORT OUT = &output_table.
            DATAFILE= "&excel_path." 
            DBMS=EXCEL REPLACE;
     	RANGE="&sheet_name."; 
     	GETNAMES=YES;
     	MIXED=NO;
     	SCANTEXT=NO;
     	USEDATE=YES;
     	SCANTIME=YES;
	RUN;
%MEND read_from_excel;


/** 模块4：输出到excel文件中 */
/** 不允许replace */
%MACRO output_to_excel(excel_path, input_table, sheet_name = data);
	LIBNAME myxls "&excel_path.";  /* external file */
		DATA myxls.&sheet_name.;
			SET &input_table.;
		RUN;
	LIBNAME myxls CLEAR;
%MEND output_to_excel;

/** 模块5: 画正态图 */
%MACRO plot_normal(var,data);
	proc univariate data=&data. normal; 
    	var &var.;
    	histogram &var.; 
    	probplot &var.;
	run;
%MEND plot_normal;


/** 模块6: 计算分布情况 */
/** 输入:
(1) input_table: &by_var. / &cal_var.
(2) by_var: 分组变量，多个用空格分离
(3) cal_var: 统计变量(单变量)
**/

%MACRO cal_dist(input_table, by_var, cal_var, out_table=stat, pctlpts=100 90 75 50 25 10 0);
	PROC SORT DATA = &input_table.;
		BY &by_var.
		;
	RUN;
	PROC UNIVARIATE DATA = &input_table. NOPRINT;
		BY &by_var.
		;
		VAR &cal_var.
		;
		OUTPUT OUT = &out_table. N = obs mean = mean std = std pctlpts = &pctlpts.
		pctlpre = p;
	QUIT;
%MEND cal_dist;


/** 模块7: 判断不同股票组合之间的重叠度 */
/** 输入:
(1) input_table: end_date/stock_code
(2) cmp_table; end_date/ stock_code
(3) is_strict: 1-要求日期完全吻合 0-允许往前寻找cmp_table中最近的日期
(4) mark_col(character): 标注变量 1- 在cmp_table中 0-不在

输出：output_table: input_table + &mark_col.
**/

%MACRO mark_in_table(input_table, cmp_table, mark_col, output_table, is_strict=0);
	%IF %SYSEVALF(&is_strict.=0) %THEN %DO;
		PROC SQL;
			CREATE TABLE date_input AS
			SELECT distinct end_date
			FROM &input_table.
			ORDER BY end_Date;
		QUIT;
		PROC SQL;
			CREATE TABLE date_cmp AS
			SELECT distinct end_date
			FROM &cmp_table.
			ORDER BY end_date;
		QUIT;
		%adjust_date_to_mapdate(rawdate_table=date_input, mapdate_table=date_cmp, 
			raw_colname=end_date, map_colname=end_date, output_table=tt_result,is_backward=1, is_included=1);
		PROC SQL;
			CREATE TABLE tt_input AS
			SELECT A.*, B.map_end_date
			FROM &input_table. A LEFT JOIN tt_result B
			ON A.end_date = B.end_date
			ORDER BY A.end_Date, A.stock_code;
		QUIT;
		PROC SQL;
			DROP TABLE date_input, date_cmp, tt_result;
		QUIT;
	%END;
	%ELSE %DO;
		DATA tt_input;
			SET &input_table.;
			map_end_Date =end_date;
		RUN;
	%END;
	
	/** 重合度 */
	PROC SQL;
		CREATE TABLE &output_table. AS
		SELECT A.*, B.stock_code AS stock_code_b
		FROM tt_input A LEFT JOIN &cmp_table. B
		ON A.map_end_date = B.end_date AND A.stock_code = B.stock_code
		ORDER BY A.end_date, A.stock_code;
	QUIT;

	DATA &output_table.(drop = stock_code_b map_end_date);
		SET &output_table.;
		IF not missing(stock_code_b) THEN &mark_col. = 1;
		ELSE &mark_col. = 0;
	RUN;
	PROC SQL;
		DROP TABLE tt_input;
	QUIT;
%MEND mark_in_table;

/** 模块8: 生成宏变量 */
%MACRO gen_macro_var_list(input_table, var_name, var_macro, nobs_macro);
	PROC SQL NOPRINT;
          SELECT distinct &var_name., count(distinct &var_name.)
          INTO :&var_macro. SEPARATED BY ' ',
               :&nobs_macro.
          FROM &input_table.;
     QUIT;
%MEND gen_macro_var_list;

/** 模块9: 取间隔N期的变量值 */
/**
(1) input_table: 
(2) identity: 主体标识，可以是stock_code等
(3) raw_col: 需要提取的变量名
(4) date_col: 日期列
(5) output_col: 输出后，对应的变量名
(6) offset: 向前(负数)/向后(正数)几期
**/
%MACRO get_nearby_data(input_table, identity, raw_col,date_col, output_col, offset, output_table);
	PROC SQL;
		CREATE TABLE tt_date AS
		SELECT distinct &date_col.
		FROM &input_table.
		ORDER BY &date_col.;
	QUIT;
	DATA tt_date;
		SET tt_date;
		id = _N_;
	RUN;
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.id AS date_id
		FROM &input_table. A LEFT JOIN tt_date B
		ON A.&date_col. = B.&date_col.
		ORDER BY date_id;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.&raw_col. AS &output_col.
		FROM tmp A LEFT JOIN tmp B
		ON A.&identity. = B.&identity. AND B.date_id - A.date_id = &offset.
		ORDER BY  A.&identity., A.&date_col.;
	QUIT;
	DATA &output_table.;
		SET tmp2;
		DROP date_id;
	RUN;
	PROC SQL;
		DROP TABLE tt_date, tmp, tmp2;
	QUIT;
%MEND get_nearby_data;

%MACRO output_to_csv(csv_path, input_table);
	PROC EXPORT DATA = &input_table.
		OUTFILE = "&csv_path."
		DBMS = CSV
		REPLACE;
	RUN;
%MEND output_to_csv;

  
