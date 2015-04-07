/** ======================说明=================================================**/
/*** 函数功能：提供与日期无关的辅助的通用函数 **** /

/**** 函数列表:
(1) get_sector_info: 提取行业信息
(2) get_stock_size: 提取市值、流通市值信息等
****/ 

/** =======================================================================**/



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
		ON A.stock_code = B.stock_code AND A.end_date = B.end_date   
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
(2) index(numeric): 可以取 1-freeshare 2-a_share 3-total_share中一者。
(3) info_table: stock_code/end_date/close
(4) share_table: stock_code/end_date/freeshare(a_share,total_share等) 

/** 输出:
(1) output_stock_table: end_date/stock_code/value  **/
%MACRO get_stock_size(stock_table, info_table, share_table,output_table,  index = 1);
	%IF %SYSEVALF(&index. = 1) %THEN %DO;
		%LET var_name = freeshare;
	%END;
	%ELSE %IF %SYSEVALF(&index. = 2) %THEN %DO;
		%LET var_name = a_share;
	%END;
	%ELSE %IF %SYSEVALF(&index. = 3) %THEN %DO;
		%LET var_name = total_share;
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
		value = close * &var_name.;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND get_stock_size;
