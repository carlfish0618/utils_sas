/** ======================说明=================================================**/
/*** 函数功能：提供与日期无关的辅助的通用函数 **** /

/**** 函数列表:
(1) get_sector_info: 提取行业信息
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
		CREATE TABLE &output_stock_table. AS
		SELECT A.*, B.indus_code, B.indus_name
		FROM &stock_table. A LEFT JOIN &mapping_table. B
		ON A.stock_code = B.stock_code AND A.end_date = B.end_date   
		ORDER BY A.end_date, B.indus_code;
	QUIT;

%MEND get_sector_info;
