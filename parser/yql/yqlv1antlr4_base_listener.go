// Code generated from YQLv1Antlr4.g4 by ANTLR 4.13.2. DO NOT EDIT.

package yql // YQLv1Antlr4
import "github.com/antlr4-go/antlr/v4"

// BaseYQLv1Antlr4Listener is a complete listener for a parse tree produced by YQLv1Antlr4Parser.
type BaseYQLv1Antlr4Listener struct{}

var _ YQLv1Antlr4Listener = &BaseYQLv1Antlr4Listener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseYQLv1Antlr4Listener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseYQLv1Antlr4Listener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseYQLv1Antlr4Listener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseYQLv1Antlr4Listener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterSql_query is called when production sql_query is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSql_query(ctx *Sql_queryContext) {}

// ExitSql_query is called when production sql_query is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSql_query(ctx *Sql_queryContext) {}

// EnterSql_stmt_list is called when production sql_stmt_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSql_stmt_list(ctx *Sql_stmt_listContext) {}

// ExitSql_stmt_list is called when production sql_stmt_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSql_stmt_list(ctx *Sql_stmt_listContext) {}

// EnterAnsi_sql_stmt_list is called when production ansi_sql_stmt_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAnsi_sql_stmt_list(ctx *Ansi_sql_stmt_listContext) {}

// ExitAnsi_sql_stmt_list is called when production ansi_sql_stmt_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAnsi_sql_stmt_list(ctx *Ansi_sql_stmt_listContext) {}

// EnterLambda_body is called when production lambda_body is entered.
func (s *BaseYQLv1Antlr4Listener) EnterLambda_body(ctx *Lambda_bodyContext) {}

// ExitLambda_body is called when production lambda_body is exited.
func (s *BaseYQLv1Antlr4Listener) ExitLambda_body(ctx *Lambda_bodyContext) {}

// EnterLambda_stmt is called when production lambda_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterLambda_stmt(ctx *Lambda_stmtContext) {}

// ExitLambda_stmt is called when production lambda_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitLambda_stmt(ctx *Lambda_stmtContext) {}

// EnterSql_stmt is called when production sql_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSql_stmt(ctx *Sql_stmtContext) {}

// ExitSql_stmt is called when production sql_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSql_stmt(ctx *Sql_stmtContext) {}

// EnterSql_stmt_core is called when production sql_stmt_core is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSql_stmt_core(ctx *Sql_stmt_coreContext) {}

// ExitSql_stmt_core is called when production sql_stmt_core is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSql_stmt_core(ctx *Sql_stmt_coreContext) {}

// EnterExpr is called when production expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterExpr(ctx *ExprContext) {}

// ExitExpr is called when production expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitExpr(ctx *ExprContext) {}

// EnterOr_subexpr is called when production or_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterOr_subexpr(ctx *Or_subexprContext) {}

// ExitOr_subexpr is called when production or_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitOr_subexpr(ctx *Or_subexprContext) {}

// EnterAnd_subexpr is called when production and_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAnd_subexpr(ctx *And_subexprContext) {}

// ExitAnd_subexpr is called when production and_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAnd_subexpr(ctx *And_subexprContext) {}

// EnterXor_subexpr is called when production xor_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterXor_subexpr(ctx *Xor_subexprContext) {}

// ExitXor_subexpr is called when production xor_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitXor_subexpr(ctx *Xor_subexprContext) {}

// EnterDistinct_from_op is called when production distinct_from_op is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDistinct_from_op(ctx *Distinct_from_opContext) {}

// ExitDistinct_from_op is called when production distinct_from_op is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDistinct_from_op(ctx *Distinct_from_opContext) {}

// EnterCond_expr is called when production cond_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCond_expr(ctx *Cond_exprContext) {}

// ExitCond_expr is called when production cond_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCond_expr(ctx *Cond_exprContext) {}

// EnterMatch_op is called when production match_op is entered.
func (s *BaseYQLv1Antlr4Listener) EnterMatch_op(ctx *Match_opContext) {}

// ExitMatch_op is called when production match_op is exited.
func (s *BaseYQLv1Antlr4Listener) ExitMatch_op(ctx *Match_opContext) {}

// EnterEq_subexpr is called when production eq_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterEq_subexpr(ctx *Eq_subexprContext) {}

// ExitEq_subexpr is called when production eq_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitEq_subexpr(ctx *Eq_subexprContext) {}

// EnterShift_right is called when production shift_right is entered.
func (s *BaseYQLv1Antlr4Listener) EnterShift_right(ctx *Shift_rightContext) {}

// ExitShift_right is called when production shift_right is exited.
func (s *BaseYQLv1Antlr4Listener) ExitShift_right(ctx *Shift_rightContext) {}

// EnterRot_right is called when production rot_right is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRot_right(ctx *Rot_rightContext) {}

// ExitRot_right is called when production rot_right is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRot_right(ctx *Rot_rightContext) {}

// EnterDouble_question is called when production double_question is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDouble_question(ctx *Double_questionContext) {}

// ExitDouble_question is called when production double_question is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDouble_question(ctx *Double_questionContext) {}

// EnterNeq_subexpr is called when production neq_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterNeq_subexpr(ctx *Neq_subexprContext) {}

// ExitNeq_subexpr is called when production neq_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitNeq_subexpr(ctx *Neq_subexprContext) {}

// EnterBit_subexpr is called when production bit_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterBit_subexpr(ctx *Bit_subexprContext) {}

// ExitBit_subexpr is called when production bit_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitBit_subexpr(ctx *Bit_subexprContext) {}

// EnterAdd_subexpr is called when production add_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAdd_subexpr(ctx *Add_subexprContext) {}

// ExitAdd_subexpr is called when production add_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAdd_subexpr(ctx *Add_subexprContext) {}

// EnterMul_subexpr is called when production mul_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterMul_subexpr(ctx *Mul_subexprContext) {}

// ExitMul_subexpr is called when production mul_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitMul_subexpr(ctx *Mul_subexprContext) {}

// EnterCon_subexpr is called when production con_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCon_subexpr(ctx *Con_subexprContext) {}

// ExitCon_subexpr is called when production con_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCon_subexpr(ctx *Con_subexprContext) {}

// EnterUnary_op is called when production unary_op is entered.
func (s *BaseYQLv1Antlr4Listener) EnterUnary_op(ctx *Unary_opContext) {}

// ExitUnary_op is called when production unary_op is exited.
func (s *BaseYQLv1Antlr4Listener) ExitUnary_op(ctx *Unary_opContext) {}

// EnterUnary_subexpr_suffix is called when production unary_subexpr_suffix is entered.
func (s *BaseYQLv1Antlr4Listener) EnterUnary_subexpr_suffix(ctx *Unary_subexpr_suffixContext) {}

// ExitUnary_subexpr_suffix is called when production unary_subexpr_suffix is exited.
func (s *BaseYQLv1Antlr4Listener) ExitUnary_subexpr_suffix(ctx *Unary_subexpr_suffixContext) {}

// EnterUnary_casual_subexpr is called when production unary_casual_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterUnary_casual_subexpr(ctx *Unary_casual_subexprContext) {}

// ExitUnary_casual_subexpr is called when production unary_casual_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitUnary_casual_subexpr(ctx *Unary_casual_subexprContext) {}

// EnterIn_unary_casual_subexpr is called when production in_unary_casual_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterIn_unary_casual_subexpr(ctx *In_unary_casual_subexprContext) {}

// ExitIn_unary_casual_subexpr is called when production in_unary_casual_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitIn_unary_casual_subexpr(ctx *In_unary_casual_subexprContext) {}

// EnterUnary_subexpr is called when production unary_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterUnary_subexpr(ctx *Unary_subexprContext) {}

// ExitUnary_subexpr is called when production unary_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitUnary_subexpr(ctx *Unary_subexprContext) {}

// EnterIn_unary_subexpr is called when production in_unary_subexpr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterIn_unary_subexpr(ctx *In_unary_subexprContext) {}

// ExitIn_unary_subexpr is called when production in_unary_subexpr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitIn_unary_subexpr(ctx *In_unary_subexprContext) {}

// EnterList_literal is called when production list_literal is entered.
func (s *BaseYQLv1Antlr4Listener) EnterList_literal(ctx *List_literalContext) {}

// ExitList_literal is called when production list_literal is exited.
func (s *BaseYQLv1Antlr4Listener) ExitList_literal(ctx *List_literalContext) {}

// EnterExpr_dict_list is called when production expr_dict_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterExpr_dict_list(ctx *Expr_dict_listContext) {}

// ExitExpr_dict_list is called when production expr_dict_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitExpr_dict_list(ctx *Expr_dict_listContext) {}

// EnterDict_literal is called when production dict_literal is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDict_literal(ctx *Dict_literalContext) {}

// ExitDict_literal is called when production dict_literal is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDict_literal(ctx *Dict_literalContext) {}

// EnterExpr_struct_list is called when production expr_struct_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterExpr_struct_list(ctx *Expr_struct_listContext) {}

// ExitExpr_struct_list is called when production expr_struct_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitExpr_struct_list(ctx *Expr_struct_listContext) {}

// EnterStruct_literal is called when production struct_literal is entered.
func (s *BaseYQLv1Antlr4Listener) EnterStruct_literal(ctx *Struct_literalContext) {}

// ExitStruct_literal is called when production struct_literal is exited.
func (s *BaseYQLv1Antlr4Listener) ExitStruct_literal(ctx *Struct_literalContext) {}

// EnterAtom_expr is called when production atom_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAtom_expr(ctx *Atom_exprContext) {}

// ExitAtom_expr is called when production atom_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAtom_expr(ctx *Atom_exprContext) {}

// EnterIn_atom_expr is called when production in_atom_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterIn_atom_expr(ctx *In_atom_exprContext) {}

// ExitIn_atom_expr is called when production in_atom_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitIn_atom_expr(ctx *In_atom_exprContext) {}

// EnterCast_expr is called when production cast_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCast_expr(ctx *Cast_exprContext) {}

// ExitCast_expr is called when production cast_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCast_expr(ctx *Cast_exprContext) {}

// EnterBitcast_expr is called when production bitcast_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterBitcast_expr(ctx *Bitcast_exprContext) {}

// ExitBitcast_expr is called when production bitcast_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitBitcast_expr(ctx *Bitcast_exprContext) {}

// EnterExists_expr is called when production exists_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterExists_expr(ctx *Exists_exprContext) {}

// ExitExists_expr is called when production exists_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitExists_expr(ctx *Exists_exprContext) {}

// EnterCase_expr is called when production case_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCase_expr(ctx *Case_exprContext) {}

// ExitCase_expr is called when production case_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCase_expr(ctx *Case_exprContext) {}

// EnterLambda is called when production lambda is entered.
func (s *BaseYQLv1Antlr4Listener) EnterLambda(ctx *LambdaContext) {}

// ExitLambda is called when production lambda is exited.
func (s *BaseYQLv1Antlr4Listener) ExitLambda(ctx *LambdaContext) {}

// EnterIn_expr is called when production in_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterIn_expr(ctx *In_exprContext) {}

// ExitIn_expr is called when production in_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitIn_expr(ctx *In_exprContext) {}

// EnterJson_api_expr is called when production json_api_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_api_expr(ctx *Json_api_exprContext) {}

// ExitJson_api_expr is called when production json_api_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_api_expr(ctx *Json_api_exprContext) {}

// EnterJsonpath_spec is called when production jsonpath_spec is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJsonpath_spec(ctx *Jsonpath_specContext) {}

// ExitJsonpath_spec is called when production jsonpath_spec is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJsonpath_spec(ctx *Jsonpath_specContext) {}

// EnterJson_variable_name is called when production json_variable_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_variable_name(ctx *Json_variable_nameContext) {}

// ExitJson_variable_name is called when production json_variable_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_variable_name(ctx *Json_variable_nameContext) {}

// EnterJson_variable is called when production json_variable is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_variable(ctx *Json_variableContext) {}

// ExitJson_variable is called when production json_variable is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_variable(ctx *Json_variableContext) {}

// EnterJson_variables is called when production json_variables is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_variables(ctx *Json_variablesContext) {}

// ExitJson_variables is called when production json_variables is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_variables(ctx *Json_variablesContext) {}

// EnterJson_common_args is called when production json_common_args is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_common_args(ctx *Json_common_argsContext) {}

// ExitJson_common_args is called when production json_common_args is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_common_args(ctx *Json_common_argsContext) {}

// EnterJson_case_handler is called when production json_case_handler is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_case_handler(ctx *Json_case_handlerContext) {}

// ExitJson_case_handler is called when production json_case_handler is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_case_handler(ctx *Json_case_handlerContext) {}

// EnterJson_value is called when production json_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_value(ctx *Json_valueContext) {}

// ExitJson_value is called when production json_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_value(ctx *Json_valueContext) {}

// EnterJson_exists_handler is called when production json_exists_handler is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_exists_handler(ctx *Json_exists_handlerContext) {}

// ExitJson_exists_handler is called when production json_exists_handler is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_exists_handler(ctx *Json_exists_handlerContext) {}

// EnterJson_exists is called when production json_exists is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_exists(ctx *Json_existsContext) {}

// ExitJson_exists is called when production json_exists is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_exists(ctx *Json_existsContext) {}

// EnterJson_query_wrapper is called when production json_query_wrapper is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_query_wrapper(ctx *Json_query_wrapperContext) {}

// ExitJson_query_wrapper is called when production json_query_wrapper is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_query_wrapper(ctx *Json_query_wrapperContext) {}

// EnterJson_query_handler is called when production json_query_handler is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_query_handler(ctx *Json_query_handlerContext) {}

// ExitJson_query_handler is called when production json_query_handler is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_query_handler(ctx *Json_query_handlerContext) {}

// EnterJson_query is called when production json_query is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJson_query(ctx *Json_queryContext) {}

// ExitJson_query is called when production json_query is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJson_query(ctx *Json_queryContext) {}

// EnterSmart_parenthesis is called when production smart_parenthesis is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSmart_parenthesis(ctx *Smart_parenthesisContext) {}

// ExitSmart_parenthesis is called when production smart_parenthesis is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSmart_parenthesis(ctx *Smart_parenthesisContext) {}

// EnterExpr_list is called when production expr_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterExpr_list(ctx *Expr_listContext) {}

// ExitExpr_list is called when production expr_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitExpr_list(ctx *Expr_listContext) {}

// EnterPure_column_list is called when production pure_column_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterPure_column_list(ctx *Pure_column_listContext) {}

// ExitPure_column_list is called when production pure_column_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitPure_column_list(ctx *Pure_column_listContext) {}

// EnterPure_column_or_named is called when production pure_column_or_named is entered.
func (s *BaseYQLv1Antlr4Listener) EnterPure_column_or_named(ctx *Pure_column_or_namedContext) {}

// ExitPure_column_or_named is called when production pure_column_or_named is exited.
func (s *BaseYQLv1Antlr4Listener) ExitPure_column_or_named(ctx *Pure_column_or_namedContext) {}

// EnterPure_column_or_named_list is called when production pure_column_or_named_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterPure_column_or_named_list(ctx *Pure_column_or_named_listContext) {}

// ExitPure_column_or_named_list is called when production pure_column_or_named_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitPure_column_or_named_list(ctx *Pure_column_or_named_listContext) {}

// EnterColumn_name is called when production column_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterColumn_name(ctx *Column_nameContext) {}

// ExitColumn_name is called when production column_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitColumn_name(ctx *Column_nameContext) {}

// EnterWithout_column_name is called when production without_column_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWithout_column_name(ctx *Without_column_nameContext) {}

// ExitWithout_column_name is called when production without_column_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWithout_column_name(ctx *Without_column_nameContext) {}

// EnterColumn_list is called when production column_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterColumn_list(ctx *Column_listContext) {}

// ExitColumn_list is called when production column_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitColumn_list(ctx *Column_listContext) {}

// EnterWithout_column_list is called when production without_column_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWithout_column_list(ctx *Without_column_listContext) {}

// ExitWithout_column_list is called when production without_column_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWithout_column_list(ctx *Without_column_listContext) {}

// EnterNamed_expr is called when production named_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterNamed_expr(ctx *Named_exprContext) {}

// ExitNamed_expr is called when production named_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitNamed_expr(ctx *Named_exprContext) {}

// EnterNamed_expr_list is called when production named_expr_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterNamed_expr_list(ctx *Named_expr_listContext) {}

// ExitNamed_expr_list is called when production named_expr_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitNamed_expr_list(ctx *Named_expr_listContext) {}

// EnterInvoke_expr is called when production invoke_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterInvoke_expr(ctx *Invoke_exprContext) {}

// ExitInvoke_expr is called when production invoke_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitInvoke_expr(ctx *Invoke_exprContext) {}

// EnterInvoke_expr_tail is called when production invoke_expr_tail is entered.
func (s *BaseYQLv1Antlr4Listener) EnterInvoke_expr_tail(ctx *Invoke_expr_tailContext) {}

// ExitInvoke_expr_tail is called when production invoke_expr_tail is exited.
func (s *BaseYQLv1Antlr4Listener) ExitInvoke_expr_tail(ctx *Invoke_expr_tailContext) {}

// EnterUsing_call_expr is called when production using_call_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterUsing_call_expr(ctx *Using_call_exprContext) {}

// ExitUsing_call_expr is called when production using_call_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitUsing_call_expr(ctx *Using_call_exprContext) {}

// EnterKey_expr is called when production key_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKey_expr(ctx *Key_exprContext) {}

// ExitKey_expr is called when production key_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKey_expr(ctx *Key_exprContext) {}

// EnterWhen_expr is called when production when_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWhen_expr(ctx *When_exprContext) {}

// ExitWhen_expr is called when production when_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWhen_expr(ctx *When_exprContext) {}

// EnterLiteral_value is called when production literal_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterLiteral_value(ctx *Literal_valueContext) {}

// ExitLiteral_value is called when production literal_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitLiteral_value(ctx *Literal_valueContext) {}

// EnterBind_parameter is called when production bind_parameter is entered.
func (s *BaseYQLv1Antlr4Listener) EnterBind_parameter(ctx *Bind_parameterContext) {}

// ExitBind_parameter is called when production bind_parameter is exited.
func (s *BaseYQLv1Antlr4Listener) ExitBind_parameter(ctx *Bind_parameterContext) {}

// EnterOpt_bind_parameter is called when production opt_bind_parameter is entered.
func (s *BaseYQLv1Antlr4Listener) EnterOpt_bind_parameter(ctx *Opt_bind_parameterContext) {}

// ExitOpt_bind_parameter is called when production opt_bind_parameter is exited.
func (s *BaseYQLv1Antlr4Listener) ExitOpt_bind_parameter(ctx *Opt_bind_parameterContext) {}

// EnterBind_parameter_list is called when production bind_parameter_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterBind_parameter_list(ctx *Bind_parameter_listContext) {}

// ExitBind_parameter_list is called when production bind_parameter_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitBind_parameter_list(ctx *Bind_parameter_listContext) {}

// EnterNamed_bind_parameter is called when production named_bind_parameter is entered.
func (s *BaseYQLv1Antlr4Listener) EnterNamed_bind_parameter(ctx *Named_bind_parameterContext) {}

// ExitNamed_bind_parameter is called when production named_bind_parameter is exited.
func (s *BaseYQLv1Antlr4Listener) ExitNamed_bind_parameter(ctx *Named_bind_parameterContext) {}

// EnterNamed_bind_parameter_list is called when production named_bind_parameter_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterNamed_bind_parameter_list(ctx *Named_bind_parameter_listContext) {}

// ExitNamed_bind_parameter_list is called when production named_bind_parameter_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitNamed_bind_parameter_list(ctx *Named_bind_parameter_listContext) {}

// EnterSigned_number is called when production signed_number is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSigned_number(ctx *Signed_numberContext) {}

// ExitSigned_number is called when production signed_number is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSigned_number(ctx *Signed_numberContext) {}

// EnterType_name_simple is called when production type_name_simple is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_simple(ctx *Type_name_simpleContext) {}

// ExitType_name_simple is called when production type_name_simple is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_simple(ctx *Type_name_simpleContext) {}

// EnterInteger_or_bind is called when production integer_or_bind is entered.
func (s *BaseYQLv1Antlr4Listener) EnterInteger_or_bind(ctx *Integer_or_bindContext) {}

// ExitInteger_or_bind is called when production integer_or_bind is exited.
func (s *BaseYQLv1Antlr4Listener) ExitInteger_or_bind(ctx *Integer_or_bindContext) {}

// EnterType_name_tag is called when production type_name_tag is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_tag(ctx *Type_name_tagContext) {}

// ExitType_name_tag is called when production type_name_tag is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_tag(ctx *Type_name_tagContext) {}

// EnterStruct_arg is called when production struct_arg is entered.
func (s *BaseYQLv1Antlr4Listener) EnterStruct_arg(ctx *Struct_argContext) {}

// ExitStruct_arg is called when production struct_arg is exited.
func (s *BaseYQLv1Antlr4Listener) ExitStruct_arg(ctx *Struct_argContext) {}

// EnterStruct_arg_positional is called when production struct_arg_positional is entered.
func (s *BaseYQLv1Antlr4Listener) EnterStruct_arg_positional(ctx *Struct_arg_positionalContext) {}

// ExitStruct_arg_positional is called when production struct_arg_positional is exited.
func (s *BaseYQLv1Antlr4Listener) ExitStruct_arg_positional(ctx *Struct_arg_positionalContext) {}

// EnterVariant_arg is called when production variant_arg is entered.
func (s *BaseYQLv1Antlr4Listener) EnterVariant_arg(ctx *Variant_argContext) {}

// ExitVariant_arg is called when production variant_arg is exited.
func (s *BaseYQLv1Antlr4Listener) ExitVariant_arg(ctx *Variant_argContext) {}

// EnterCallable_arg is called when production callable_arg is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCallable_arg(ctx *Callable_argContext) {}

// ExitCallable_arg is called when production callable_arg is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCallable_arg(ctx *Callable_argContext) {}

// EnterCallable_arg_list is called when production callable_arg_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCallable_arg_list(ctx *Callable_arg_listContext) {}

// ExitCallable_arg_list is called when production callable_arg_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCallable_arg_list(ctx *Callable_arg_listContext) {}

// EnterType_name_decimal is called when production type_name_decimal is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_decimal(ctx *Type_name_decimalContext) {}

// ExitType_name_decimal is called when production type_name_decimal is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_decimal(ctx *Type_name_decimalContext) {}

// EnterType_name_optional is called when production type_name_optional is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_optional(ctx *Type_name_optionalContext) {}

// ExitType_name_optional is called when production type_name_optional is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_optional(ctx *Type_name_optionalContext) {}

// EnterType_name_tuple is called when production type_name_tuple is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_tuple(ctx *Type_name_tupleContext) {}

// ExitType_name_tuple is called when production type_name_tuple is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_tuple(ctx *Type_name_tupleContext) {}

// EnterType_name_struct is called when production type_name_struct is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_struct(ctx *Type_name_structContext) {}

// ExitType_name_struct is called when production type_name_struct is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_struct(ctx *Type_name_structContext) {}

// EnterType_name_variant is called when production type_name_variant is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_variant(ctx *Type_name_variantContext) {}

// ExitType_name_variant is called when production type_name_variant is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_variant(ctx *Type_name_variantContext) {}

// EnterType_name_list is called when production type_name_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_list(ctx *Type_name_listContext) {}

// ExitType_name_list is called when production type_name_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_list(ctx *Type_name_listContext) {}

// EnterType_name_stream is called when production type_name_stream is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_stream(ctx *Type_name_streamContext) {}

// ExitType_name_stream is called when production type_name_stream is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_stream(ctx *Type_name_streamContext) {}

// EnterType_name_flow is called when production type_name_flow is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_flow(ctx *Type_name_flowContext) {}

// ExitType_name_flow is called when production type_name_flow is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_flow(ctx *Type_name_flowContext) {}

// EnterType_name_dict is called when production type_name_dict is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_dict(ctx *Type_name_dictContext) {}

// ExitType_name_dict is called when production type_name_dict is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_dict(ctx *Type_name_dictContext) {}

// EnterType_name_set is called when production type_name_set is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_set(ctx *Type_name_setContext) {}

// ExitType_name_set is called when production type_name_set is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_set(ctx *Type_name_setContext) {}

// EnterType_name_enum is called when production type_name_enum is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_enum(ctx *Type_name_enumContext) {}

// ExitType_name_enum is called when production type_name_enum is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_enum(ctx *Type_name_enumContext) {}

// EnterType_name_resource is called when production type_name_resource is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_resource(ctx *Type_name_resourceContext) {}

// ExitType_name_resource is called when production type_name_resource is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_resource(ctx *Type_name_resourceContext) {}

// EnterType_name_tagged is called when production type_name_tagged is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_tagged(ctx *Type_name_taggedContext) {}

// ExitType_name_tagged is called when production type_name_tagged is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_tagged(ctx *Type_name_taggedContext) {}

// EnterType_name_callable is called when production type_name_callable is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_callable(ctx *Type_name_callableContext) {}

// ExitType_name_callable is called when production type_name_callable is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_callable(ctx *Type_name_callableContext) {}

// EnterType_name_composite is called when production type_name_composite is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_composite(ctx *Type_name_compositeContext) {}

// ExitType_name_composite is called when production type_name_composite is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_composite(ctx *Type_name_compositeContext) {}

// EnterType_name is called when production type_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name(ctx *Type_nameContext) {}

// ExitType_name is called when production type_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name(ctx *Type_nameContext) {}

// EnterType_name_or_bind is called when production type_name_or_bind is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_name_or_bind(ctx *Type_name_or_bindContext) {}

// ExitType_name_or_bind is called when production type_name_or_bind is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_name_or_bind(ctx *Type_name_or_bindContext) {}

// EnterValue_constructor_literal is called when production value_constructor_literal is entered.
func (s *BaseYQLv1Antlr4Listener) EnterValue_constructor_literal(ctx *Value_constructor_literalContext) {}

// ExitValue_constructor_literal is called when production value_constructor_literal is exited.
func (s *BaseYQLv1Antlr4Listener) ExitValue_constructor_literal(ctx *Value_constructor_literalContext) {}

// EnterValue_constructor is called when production value_constructor is entered.
func (s *BaseYQLv1Antlr4Listener) EnterValue_constructor(ctx *Value_constructorContext) {}

// ExitValue_constructor is called when production value_constructor is exited.
func (s *BaseYQLv1Antlr4Listener) ExitValue_constructor(ctx *Value_constructorContext) {}

// EnterDeclare_stmt is called when production declare_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDeclare_stmt(ctx *Declare_stmtContext) {}

// ExitDeclare_stmt is called when production declare_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDeclare_stmt(ctx *Declare_stmtContext) {}

// EnterModule_path is called when production module_path is entered.
func (s *BaseYQLv1Antlr4Listener) EnterModule_path(ctx *Module_pathContext) {}

// ExitModule_path is called when production module_path is exited.
func (s *BaseYQLv1Antlr4Listener) ExitModule_path(ctx *Module_pathContext) {}

// EnterImport_stmt is called when production import_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterImport_stmt(ctx *Import_stmtContext) {}

// ExitImport_stmt is called when production import_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitImport_stmt(ctx *Import_stmtContext) {}

// EnterExport_stmt is called when production export_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterExport_stmt(ctx *Export_stmtContext) {}

// ExitExport_stmt is called when production export_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitExport_stmt(ctx *Export_stmtContext) {}

// EnterCall_action is called when production call_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCall_action(ctx *Call_actionContext) {}

// ExitCall_action is called when production call_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCall_action(ctx *Call_actionContext) {}

// EnterInline_action is called when production inline_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterInline_action(ctx *Inline_actionContext) {}

// ExitInline_action is called when production inline_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitInline_action(ctx *Inline_actionContext) {}

// EnterDo_stmt is called when production do_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDo_stmt(ctx *Do_stmtContext) {}

// ExitDo_stmt is called when production do_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDo_stmt(ctx *Do_stmtContext) {}

// EnterPragma_stmt is called when production pragma_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterPragma_stmt(ctx *Pragma_stmtContext) {}

// ExitPragma_stmt is called when production pragma_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitPragma_stmt(ctx *Pragma_stmtContext) {}

// EnterPragma_value is called when production pragma_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterPragma_value(ctx *Pragma_valueContext) {}

// ExitPragma_value is called when production pragma_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitPragma_value(ctx *Pragma_valueContext) {}

// EnterSort_specification is called when production sort_specification is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSort_specification(ctx *Sort_specificationContext) {}

// ExitSort_specification is called when production sort_specification is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSort_specification(ctx *Sort_specificationContext) {}

// EnterSort_specification_list is called when production sort_specification_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSort_specification_list(ctx *Sort_specification_listContext) {}

// ExitSort_specification_list is called when production sort_specification_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSort_specification_list(ctx *Sort_specification_listContext) {}

// EnterSelect_stmt is called when production select_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSelect_stmt(ctx *Select_stmtContext) {}

// ExitSelect_stmt is called when production select_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSelect_stmt(ctx *Select_stmtContext) {}

// EnterSelect_unparenthesized_stmt is called when production select_unparenthesized_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSelect_unparenthesized_stmt(ctx *Select_unparenthesized_stmtContext) {}

// ExitSelect_unparenthesized_stmt is called when production select_unparenthesized_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSelect_unparenthesized_stmt(ctx *Select_unparenthesized_stmtContext) {}

// EnterSelect_kind_parenthesis is called when production select_kind_parenthesis is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSelect_kind_parenthesis(ctx *Select_kind_parenthesisContext) {}

// ExitSelect_kind_parenthesis is called when production select_kind_parenthesis is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSelect_kind_parenthesis(ctx *Select_kind_parenthesisContext) {}

// EnterSelect_op is called when production select_op is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSelect_op(ctx *Select_opContext) {}

// ExitSelect_op is called when production select_op is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSelect_op(ctx *Select_opContext) {}

// EnterSelect_kind_partial is called when production select_kind_partial is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSelect_kind_partial(ctx *Select_kind_partialContext) {}

// ExitSelect_kind_partial is called when production select_kind_partial is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSelect_kind_partial(ctx *Select_kind_partialContext) {}

// EnterSelect_kind is called when production select_kind is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSelect_kind(ctx *Select_kindContext) {}

// ExitSelect_kind is called when production select_kind is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSelect_kind(ctx *Select_kindContext) {}

// EnterProcess_core is called when production process_core is entered.
func (s *BaseYQLv1Antlr4Listener) EnterProcess_core(ctx *Process_coreContext) {}

// ExitProcess_core is called when production process_core is exited.
func (s *BaseYQLv1Antlr4Listener) ExitProcess_core(ctx *Process_coreContext) {}

// EnterExternal_call_param is called when production external_call_param is entered.
func (s *BaseYQLv1Antlr4Listener) EnterExternal_call_param(ctx *External_call_paramContext) {}

// ExitExternal_call_param is called when production external_call_param is exited.
func (s *BaseYQLv1Antlr4Listener) ExitExternal_call_param(ctx *External_call_paramContext) {}

// EnterExternal_call_settings is called when production external_call_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterExternal_call_settings(ctx *External_call_settingsContext) {}

// ExitExternal_call_settings is called when production external_call_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitExternal_call_settings(ctx *External_call_settingsContext) {}

// EnterReduce_core is called when production reduce_core is entered.
func (s *BaseYQLv1Antlr4Listener) EnterReduce_core(ctx *Reduce_coreContext) {}

// ExitReduce_core is called when production reduce_core is exited.
func (s *BaseYQLv1Antlr4Listener) ExitReduce_core(ctx *Reduce_coreContext) {}

// EnterOpt_set_quantifier is called when production opt_set_quantifier is entered.
func (s *BaseYQLv1Antlr4Listener) EnterOpt_set_quantifier(ctx *Opt_set_quantifierContext) {}

// ExitOpt_set_quantifier is called when production opt_set_quantifier is exited.
func (s *BaseYQLv1Antlr4Listener) ExitOpt_set_quantifier(ctx *Opt_set_quantifierContext) {}

// EnterSelect_core is called when production select_core is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSelect_core(ctx *Select_coreContext) {}

// ExitSelect_core is called when production select_core is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSelect_core(ctx *Select_coreContext) {}

// EnterRow_pattern_recognition_clause is called when production row_pattern_recognition_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_recognition_clause(ctx *Row_pattern_recognition_clauseContext) {}

// ExitRow_pattern_recognition_clause is called when production row_pattern_recognition_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_recognition_clause(ctx *Row_pattern_recognition_clauseContext) {}

// EnterRow_pattern_rows_per_match is called when production row_pattern_rows_per_match is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_rows_per_match(ctx *Row_pattern_rows_per_matchContext) {}

// ExitRow_pattern_rows_per_match is called when production row_pattern_rows_per_match is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_rows_per_match(ctx *Row_pattern_rows_per_matchContext) {}

// EnterRow_pattern_empty_match_handling is called when production row_pattern_empty_match_handling is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_empty_match_handling(ctx *Row_pattern_empty_match_handlingContext) {}

// ExitRow_pattern_empty_match_handling is called when production row_pattern_empty_match_handling is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_empty_match_handling(ctx *Row_pattern_empty_match_handlingContext) {}

// EnterRow_pattern_measures is called when production row_pattern_measures is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_measures(ctx *Row_pattern_measuresContext) {}

// ExitRow_pattern_measures is called when production row_pattern_measures is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_measures(ctx *Row_pattern_measuresContext) {}

// EnterRow_pattern_measure_list is called when production row_pattern_measure_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_measure_list(ctx *Row_pattern_measure_listContext) {}

// ExitRow_pattern_measure_list is called when production row_pattern_measure_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_measure_list(ctx *Row_pattern_measure_listContext) {}

// EnterRow_pattern_measure_definition is called when production row_pattern_measure_definition is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_measure_definition(ctx *Row_pattern_measure_definitionContext) {}

// ExitRow_pattern_measure_definition is called when production row_pattern_measure_definition is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_measure_definition(ctx *Row_pattern_measure_definitionContext) {}

// EnterRow_pattern_common_syntax is called when production row_pattern_common_syntax is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_common_syntax(ctx *Row_pattern_common_syntaxContext) {}

// ExitRow_pattern_common_syntax is called when production row_pattern_common_syntax is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_common_syntax(ctx *Row_pattern_common_syntaxContext) {}

// EnterRow_pattern_skip_to is called when production row_pattern_skip_to is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_skip_to(ctx *Row_pattern_skip_toContext) {}

// ExitRow_pattern_skip_to is called when production row_pattern_skip_to is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_skip_to(ctx *Row_pattern_skip_toContext) {}

// EnterRow_pattern_skip_to_variable_name is called when production row_pattern_skip_to_variable_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_skip_to_variable_name(ctx *Row_pattern_skip_to_variable_nameContext) {}

// ExitRow_pattern_skip_to_variable_name is called when production row_pattern_skip_to_variable_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_skip_to_variable_name(ctx *Row_pattern_skip_to_variable_nameContext) {}

// EnterRow_pattern_initial_or_seek is called when production row_pattern_initial_or_seek is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_initial_or_seek(ctx *Row_pattern_initial_or_seekContext) {}

// ExitRow_pattern_initial_or_seek is called when production row_pattern_initial_or_seek is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_initial_or_seek(ctx *Row_pattern_initial_or_seekContext) {}

// EnterRow_pattern is called when production row_pattern is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern(ctx *Row_patternContext) {}

// ExitRow_pattern is called when production row_pattern is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern(ctx *Row_patternContext) {}

// EnterRow_pattern_term is called when production row_pattern_term is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_term(ctx *Row_pattern_termContext) {}

// ExitRow_pattern_term is called when production row_pattern_term is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_term(ctx *Row_pattern_termContext) {}

// EnterRow_pattern_factor is called when production row_pattern_factor is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_factor(ctx *Row_pattern_factorContext) {}

// ExitRow_pattern_factor is called when production row_pattern_factor is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_factor(ctx *Row_pattern_factorContext) {}

// EnterRow_pattern_quantifier is called when production row_pattern_quantifier is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_quantifier(ctx *Row_pattern_quantifierContext) {}

// ExitRow_pattern_quantifier is called when production row_pattern_quantifier is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_quantifier(ctx *Row_pattern_quantifierContext) {}

// EnterRow_pattern_primary is called when production row_pattern_primary is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_primary(ctx *Row_pattern_primaryContext) {}

// ExitRow_pattern_primary is called when production row_pattern_primary is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_primary(ctx *Row_pattern_primaryContext) {}

// EnterRow_pattern_primary_variable_name is called when production row_pattern_primary_variable_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_primary_variable_name(ctx *Row_pattern_primary_variable_nameContext) {}

// ExitRow_pattern_primary_variable_name is called when production row_pattern_primary_variable_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_primary_variable_name(ctx *Row_pattern_primary_variable_nameContext) {}

// EnterRow_pattern_permute is called when production row_pattern_permute is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_permute(ctx *Row_pattern_permuteContext) {}

// ExitRow_pattern_permute is called when production row_pattern_permute is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_permute(ctx *Row_pattern_permuteContext) {}

// EnterRow_pattern_subset_clause is called when production row_pattern_subset_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_subset_clause(ctx *Row_pattern_subset_clauseContext) {}

// ExitRow_pattern_subset_clause is called when production row_pattern_subset_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_subset_clause(ctx *Row_pattern_subset_clauseContext) {}

// EnterRow_pattern_subset_list is called when production row_pattern_subset_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_subset_list(ctx *Row_pattern_subset_listContext) {}

// ExitRow_pattern_subset_list is called when production row_pattern_subset_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_subset_list(ctx *Row_pattern_subset_listContext) {}

// EnterRow_pattern_subset_item is called when production row_pattern_subset_item is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_subset_item(ctx *Row_pattern_subset_itemContext) {}

// ExitRow_pattern_subset_item is called when production row_pattern_subset_item is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_subset_item(ctx *Row_pattern_subset_itemContext) {}

// EnterRow_pattern_subset_item_variable_name is called when production row_pattern_subset_item_variable_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_subset_item_variable_name(ctx *Row_pattern_subset_item_variable_nameContext) {}

// ExitRow_pattern_subset_item_variable_name is called when production row_pattern_subset_item_variable_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_subset_item_variable_name(ctx *Row_pattern_subset_item_variable_nameContext) {}

// EnterRow_pattern_subset_rhs is called when production row_pattern_subset_rhs is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_subset_rhs(ctx *Row_pattern_subset_rhsContext) {}

// ExitRow_pattern_subset_rhs is called when production row_pattern_subset_rhs is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_subset_rhs(ctx *Row_pattern_subset_rhsContext) {}

// EnterRow_pattern_subset_rhs_variable_name is called when production row_pattern_subset_rhs_variable_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_subset_rhs_variable_name(ctx *Row_pattern_subset_rhs_variable_nameContext) {}

// ExitRow_pattern_subset_rhs_variable_name is called when production row_pattern_subset_rhs_variable_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_subset_rhs_variable_name(ctx *Row_pattern_subset_rhs_variable_nameContext) {}

// EnterRow_pattern_definition_list is called when production row_pattern_definition_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_definition_list(ctx *Row_pattern_definition_listContext) {}

// ExitRow_pattern_definition_list is called when production row_pattern_definition_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_definition_list(ctx *Row_pattern_definition_listContext) {}

// EnterRow_pattern_definition is called when production row_pattern_definition is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_definition(ctx *Row_pattern_definitionContext) {}

// ExitRow_pattern_definition is called when production row_pattern_definition is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_definition(ctx *Row_pattern_definitionContext) {}

// EnterRow_pattern_definition_variable_name is called when production row_pattern_definition_variable_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_definition_variable_name(ctx *Row_pattern_definition_variable_nameContext) {}

// ExitRow_pattern_definition_variable_name is called when production row_pattern_definition_variable_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_definition_variable_name(ctx *Row_pattern_definition_variable_nameContext) {}

// EnterRow_pattern_definition_search_condition is called when production row_pattern_definition_search_condition is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_definition_search_condition(ctx *Row_pattern_definition_search_conditionContext) {}

// ExitRow_pattern_definition_search_condition is called when production row_pattern_definition_search_condition is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_definition_search_condition(ctx *Row_pattern_definition_search_conditionContext) {}

// EnterSearch_condition is called when production search_condition is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSearch_condition(ctx *Search_conditionContext) {}

// ExitSearch_condition is called when production search_condition is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSearch_condition(ctx *Search_conditionContext) {}

// EnterRow_pattern_variable_name is called when production row_pattern_variable_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRow_pattern_variable_name(ctx *Row_pattern_variable_nameContext) {}

// ExitRow_pattern_variable_name is called when production row_pattern_variable_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRow_pattern_variable_name(ctx *Row_pattern_variable_nameContext) {}

// EnterOrder_by_clause is called when production order_by_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterOrder_by_clause(ctx *Order_by_clauseContext) {}

// ExitOrder_by_clause is called when production order_by_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitOrder_by_clause(ctx *Order_by_clauseContext) {}

// EnterExt_order_by_clause is called when production ext_order_by_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterExt_order_by_clause(ctx *Ext_order_by_clauseContext) {}

// ExitExt_order_by_clause is called when production ext_order_by_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitExt_order_by_clause(ctx *Ext_order_by_clauseContext) {}

// EnterGroup_by_clause is called when production group_by_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterGroup_by_clause(ctx *Group_by_clauseContext) {}

// ExitGroup_by_clause is called when production group_by_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitGroup_by_clause(ctx *Group_by_clauseContext) {}

// EnterGrouping_element_list is called when production grouping_element_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterGrouping_element_list(ctx *Grouping_element_listContext) {}

// ExitGrouping_element_list is called when production grouping_element_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitGrouping_element_list(ctx *Grouping_element_listContext) {}

// EnterGrouping_element is called when production grouping_element is entered.
func (s *BaseYQLv1Antlr4Listener) EnterGrouping_element(ctx *Grouping_elementContext) {}

// ExitGrouping_element is called when production grouping_element is exited.
func (s *BaseYQLv1Antlr4Listener) ExitGrouping_element(ctx *Grouping_elementContext) {}

// EnterOrdinary_grouping_set is called when production ordinary_grouping_set is entered.
func (s *BaseYQLv1Antlr4Listener) EnterOrdinary_grouping_set(ctx *Ordinary_grouping_setContext) {}

// ExitOrdinary_grouping_set is called when production ordinary_grouping_set is exited.
func (s *BaseYQLv1Antlr4Listener) ExitOrdinary_grouping_set(ctx *Ordinary_grouping_setContext) {}

// EnterOrdinary_grouping_set_list is called when production ordinary_grouping_set_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterOrdinary_grouping_set_list(ctx *Ordinary_grouping_set_listContext) {}

// ExitOrdinary_grouping_set_list is called when production ordinary_grouping_set_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitOrdinary_grouping_set_list(ctx *Ordinary_grouping_set_listContext) {}

// EnterRollup_list is called when production rollup_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRollup_list(ctx *Rollup_listContext) {}

// ExitRollup_list is called when production rollup_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRollup_list(ctx *Rollup_listContext) {}

// EnterCube_list is called when production cube_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCube_list(ctx *Cube_listContext) {}

// ExitCube_list is called when production cube_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCube_list(ctx *Cube_listContext) {}

// EnterGrouping_sets_specification is called when production grouping_sets_specification is entered.
func (s *BaseYQLv1Antlr4Listener) EnterGrouping_sets_specification(ctx *Grouping_sets_specificationContext) {}

// ExitGrouping_sets_specification is called when production grouping_sets_specification is exited.
func (s *BaseYQLv1Antlr4Listener) ExitGrouping_sets_specification(ctx *Grouping_sets_specificationContext) {}

// EnterHopping_window_specification is called when production hopping_window_specification is entered.
func (s *BaseYQLv1Antlr4Listener) EnterHopping_window_specification(ctx *Hopping_window_specificationContext) {}

// ExitHopping_window_specification is called when production hopping_window_specification is exited.
func (s *BaseYQLv1Antlr4Listener) ExitHopping_window_specification(ctx *Hopping_window_specificationContext) {}

// EnterResult_column is called when production result_column is entered.
func (s *BaseYQLv1Antlr4Listener) EnterResult_column(ctx *Result_columnContext) {}

// ExitResult_column is called when production result_column is exited.
func (s *BaseYQLv1Antlr4Listener) ExitResult_column(ctx *Result_columnContext) {}

// EnterJoin_source is called when production join_source is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJoin_source(ctx *Join_sourceContext) {}

// ExitJoin_source is called when production join_source is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJoin_source(ctx *Join_sourceContext) {}

// EnterNamed_column is called when production named_column is entered.
func (s *BaseYQLv1Antlr4Listener) EnterNamed_column(ctx *Named_columnContext) {}

// ExitNamed_column is called when production named_column is exited.
func (s *BaseYQLv1Antlr4Listener) ExitNamed_column(ctx *Named_columnContext) {}

// EnterFlatten_by_arg is called when production flatten_by_arg is entered.
func (s *BaseYQLv1Antlr4Listener) EnterFlatten_by_arg(ctx *Flatten_by_argContext) {}

// ExitFlatten_by_arg is called when production flatten_by_arg is exited.
func (s *BaseYQLv1Antlr4Listener) ExitFlatten_by_arg(ctx *Flatten_by_argContext) {}

// EnterFlatten_source is called when production flatten_source is entered.
func (s *BaseYQLv1Antlr4Listener) EnterFlatten_source(ctx *Flatten_sourceContext) {}

// ExitFlatten_source is called when production flatten_source is exited.
func (s *BaseYQLv1Antlr4Listener) ExitFlatten_source(ctx *Flatten_sourceContext) {}

// EnterNamed_single_source is called when production named_single_source is entered.
func (s *BaseYQLv1Antlr4Listener) EnterNamed_single_source(ctx *Named_single_sourceContext) {}

// ExitNamed_single_source is called when production named_single_source is exited.
func (s *BaseYQLv1Antlr4Listener) ExitNamed_single_source(ctx *Named_single_sourceContext) {}

// EnterSingle_source is called when production single_source is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSingle_source(ctx *Single_sourceContext) {}

// ExitSingle_source is called when production single_source is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSingle_source(ctx *Single_sourceContext) {}

// EnterSample_clause is called when production sample_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSample_clause(ctx *Sample_clauseContext) {}

// ExitSample_clause is called when production sample_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSample_clause(ctx *Sample_clauseContext) {}

// EnterTablesample_clause is called when production tablesample_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTablesample_clause(ctx *Tablesample_clauseContext) {}

// ExitTablesample_clause is called when production tablesample_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTablesample_clause(ctx *Tablesample_clauseContext) {}

// EnterSampling_mode is called when production sampling_mode is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSampling_mode(ctx *Sampling_modeContext) {}

// ExitSampling_mode is called when production sampling_mode is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSampling_mode(ctx *Sampling_modeContext) {}

// EnterRepeatable_clause is called when production repeatable_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRepeatable_clause(ctx *Repeatable_clauseContext) {}

// ExitRepeatable_clause is called when production repeatable_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRepeatable_clause(ctx *Repeatable_clauseContext) {}

// EnterJoin_op is called when production join_op is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJoin_op(ctx *Join_opContext) {}

// ExitJoin_op is called when production join_op is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJoin_op(ctx *Join_opContext) {}

// EnterJoin_constraint is called when production join_constraint is entered.
func (s *BaseYQLv1Antlr4Listener) EnterJoin_constraint(ctx *Join_constraintContext) {}

// ExitJoin_constraint is called when production join_constraint is exited.
func (s *BaseYQLv1Antlr4Listener) ExitJoin_constraint(ctx *Join_constraintContext) {}

// EnterReturning_columns_list is called when production returning_columns_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterReturning_columns_list(ctx *Returning_columns_listContext) {}

// ExitReturning_columns_list is called when production returning_columns_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitReturning_columns_list(ctx *Returning_columns_listContext) {}

// EnterInto_table_stmt is called when production into_table_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterInto_table_stmt(ctx *Into_table_stmtContext) {}

// ExitInto_table_stmt is called when production into_table_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitInto_table_stmt(ctx *Into_table_stmtContext) {}

// EnterInto_values_source is called when production into_values_source is entered.
func (s *BaseYQLv1Antlr4Listener) EnterInto_values_source(ctx *Into_values_sourceContext) {}

// ExitInto_values_source is called when production into_values_source is exited.
func (s *BaseYQLv1Antlr4Listener) ExitInto_values_source(ctx *Into_values_sourceContext) {}

// EnterValues_stmt is called when production values_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterValues_stmt(ctx *Values_stmtContext) {}

// ExitValues_stmt is called when production values_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitValues_stmt(ctx *Values_stmtContext) {}

// EnterValues_source is called when production values_source is entered.
func (s *BaseYQLv1Antlr4Listener) EnterValues_source(ctx *Values_sourceContext) {}

// ExitValues_source is called when production values_source is exited.
func (s *BaseYQLv1Antlr4Listener) ExitValues_source(ctx *Values_sourceContext) {}

// EnterValues_source_row_list is called when production values_source_row_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterValues_source_row_list(ctx *Values_source_row_listContext) {}

// ExitValues_source_row_list is called when production values_source_row_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitValues_source_row_list(ctx *Values_source_row_listContext) {}

// EnterValues_source_row is called when production values_source_row is entered.
func (s *BaseYQLv1Antlr4Listener) EnterValues_source_row(ctx *Values_source_rowContext) {}

// ExitValues_source_row is called when production values_source_row is exited.
func (s *BaseYQLv1Antlr4Listener) ExitValues_source_row(ctx *Values_source_rowContext) {}

// EnterSimple_values_source is called when production simple_values_source is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSimple_values_source(ctx *Simple_values_sourceContext) {}

// ExitSimple_values_source is called when production simple_values_source is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSimple_values_source(ctx *Simple_values_sourceContext) {}

// EnterCreate_external_data_source_stmt is called when production create_external_data_source_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_external_data_source_stmt(ctx *Create_external_data_source_stmtContext) {}

// ExitCreate_external_data_source_stmt is called when production create_external_data_source_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_external_data_source_stmt(ctx *Create_external_data_source_stmtContext) {}

// EnterAlter_external_data_source_stmt is called when production alter_external_data_source_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_external_data_source_stmt(ctx *Alter_external_data_source_stmtContext) {}

// ExitAlter_external_data_source_stmt is called when production alter_external_data_source_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_external_data_source_stmt(ctx *Alter_external_data_source_stmtContext) {}

// EnterAlter_external_data_source_action is called when production alter_external_data_source_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_external_data_source_action(ctx *Alter_external_data_source_actionContext) {}

// ExitAlter_external_data_source_action is called when production alter_external_data_source_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_external_data_source_action(ctx *Alter_external_data_source_actionContext) {}

// EnterDrop_external_data_source_stmt is called when production drop_external_data_source_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_external_data_source_stmt(ctx *Drop_external_data_source_stmtContext) {}

// ExitDrop_external_data_source_stmt is called when production drop_external_data_source_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_external_data_source_stmt(ctx *Drop_external_data_source_stmtContext) {}

// EnterCreate_view_stmt is called when production create_view_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_view_stmt(ctx *Create_view_stmtContext) {}

// ExitCreate_view_stmt is called when production create_view_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_view_stmt(ctx *Create_view_stmtContext) {}

// EnterDrop_view_stmt is called when production drop_view_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_view_stmt(ctx *Drop_view_stmtContext) {}

// ExitDrop_view_stmt is called when production drop_view_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_view_stmt(ctx *Drop_view_stmtContext) {}

// EnterUpsert_object_stmt is called when production upsert_object_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterUpsert_object_stmt(ctx *Upsert_object_stmtContext) {}

// ExitUpsert_object_stmt is called when production upsert_object_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitUpsert_object_stmt(ctx *Upsert_object_stmtContext) {}

// EnterCreate_object_stmt is called when production create_object_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_object_stmt(ctx *Create_object_stmtContext) {}

// ExitCreate_object_stmt is called when production create_object_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_object_stmt(ctx *Create_object_stmtContext) {}

// EnterCreate_object_features is called when production create_object_features is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_object_features(ctx *Create_object_featuresContext) {}

// ExitCreate_object_features is called when production create_object_features is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_object_features(ctx *Create_object_featuresContext) {}

// EnterAlter_object_stmt is called when production alter_object_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_object_stmt(ctx *Alter_object_stmtContext) {}

// ExitAlter_object_stmt is called when production alter_object_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_object_stmt(ctx *Alter_object_stmtContext) {}

// EnterAlter_object_features is called when production alter_object_features is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_object_features(ctx *Alter_object_featuresContext) {}

// ExitAlter_object_features is called when production alter_object_features is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_object_features(ctx *Alter_object_featuresContext) {}

// EnterDrop_object_stmt is called when production drop_object_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_object_stmt(ctx *Drop_object_stmtContext) {}

// ExitDrop_object_stmt is called when production drop_object_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_object_stmt(ctx *Drop_object_stmtContext) {}

// EnterDrop_object_features is called when production drop_object_features is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_object_features(ctx *Drop_object_featuresContext) {}

// ExitDrop_object_features is called when production drop_object_features is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_object_features(ctx *Drop_object_featuresContext) {}

// EnterObject_feature_value is called when production object_feature_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterObject_feature_value(ctx *Object_feature_valueContext) {}

// ExitObject_feature_value is called when production object_feature_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitObject_feature_value(ctx *Object_feature_valueContext) {}

// EnterObject_feature_kv is called when production object_feature_kv is entered.
func (s *BaseYQLv1Antlr4Listener) EnterObject_feature_kv(ctx *Object_feature_kvContext) {}

// ExitObject_feature_kv is called when production object_feature_kv is exited.
func (s *BaseYQLv1Antlr4Listener) ExitObject_feature_kv(ctx *Object_feature_kvContext) {}

// EnterObject_feature_flag is called when production object_feature_flag is entered.
func (s *BaseYQLv1Antlr4Listener) EnterObject_feature_flag(ctx *Object_feature_flagContext) {}

// ExitObject_feature_flag is called when production object_feature_flag is exited.
func (s *BaseYQLv1Antlr4Listener) ExitObject_feature_flag(ctx *Object_feature_flagContext) {}

// EnterObject_feature is called when production object_feature is entered.
func (s *BaseYQLv1Antlr4Listener) EnterObject_feature(ctx *Object_featureContext) {}

// ExitObject_feature is called when production object_feature is exited.
func (s *BaseYQLv1Antlr4Listener) ExitObject_feature(ctx *Object_featureContext) {}

// EnterObject_features is called when production object_features is entered.
func (s *BaseYQLv1Antlr4Listener) EnterObject_features(ctx *Object_featuresContext) {}

// ExitObject_features is called when production object_features is exited.
func (s *BaseYQLv1Antlr4Listener) ExitObject_features(ctx *Object_featuresContext) {}

// EnterObject_type_ref is called when production object_type_ref is entered.
func (s *BaseYQLv1Antlr4Listener) EnterObject_type_ref(ctx *Object_type_refContext) {}

// ExitObject_type_ref is called when production object_type_ref is exited.
func (s *BaseYQLv1Antlr4Listener) ExitObject_type_ref(ctx *Object_type_refContext) {}

// EnterCreate_table_stmt is called when production create_table_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_table_stmt(ctx *Create_table_stmtContext) {}

// ExitCreate_table_stmt is called when production create_table_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_table_stmt(ctx *Create_table_stmtContext) {}

// EnterCreate_table_entry is called when production create_table_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_table_entry(ctx *Create_table_entryContext) {}

// ExitCreate_table_entry is called when production create_table_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_table_entry(ctx *Create_table_entryContext) {}

// EnterCreate_backup_collection_stmt is called when production create_backup_collection_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_backup_collection_stmt(ctx *Create_backup_collection_stmtContext) {}

// ExitCreate_backup_collection_stmt is called when production create_backup_collection_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_backup_collection_stmt(ctx *Create_backup_collection_stmtContext) {}

// EnterAlter_backup_collection_stmt is called when production alter_backup_collection_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_backup_collection_stmt(ctx *Alter_backup_collection_stmtContext) {}

// ExitAlter_backup_collection_stmt is called when production alter_backup_collection_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_backup_collection_stmt(ctx *Alter_backup_collection_stmtContext) {}

// EnterDrop_backup_collection_stmt is called when production drop_backup_collection_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_backup_collection_stmt(ctx *Drop_backup_collection_stmtContext) {}

// ExitDrop_backup_collection_stmt is called when production drop_backup_collection_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_backup_collection_stmt(ctx *Drop_backup_collection_stmtContext) {}

// EnterCreate_backup_collection_entries is called when production create_backup_collection_entries is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_backup_collection_entries(ctx *Create_backup_collection_entriesContext) {}

// ExitCreate_backup_collection_entries is called when production create_backup_collection_entries is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_backup_collection_entries(ctx *Create_backup_collection_entriesContext) {}

// EnterCreate_backup_collection_entries_many is called when production create_backup_collection_entries_many is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_backup_collection_entries_many(ctx *Create_backup_collection_entries_manyContext) {}

// ExitCreate_backup_collection_entries_many is called when production create_backup_collection_entries_many is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_backup_collection_entries_many(ctx *Create_backup_collection_entries_manyContext) {}

// EnterTable_list is called when production table_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_list(ctx *Table_listContext) {}

// ExitTable_list is called when production table_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_list(ctx *Table_listContext) {}

// EnterAlter_backup_collection_actions is called when production alter_backup_collection_actions is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_backup_collection_actions(ctx *Alter_backup_collection_actionsContext) {}

// ExitAlter_backup_collection_actions is called when production alter_backup_collection_actions is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_backup_collection_actions(ctx *Alter_backup_collection_actionsContext) {}

// EnterAlter_backup_collection_action is called when production alter_backup_collection_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_backup_collection_action(ctx *Alter_backup_collection_actionContext) {}

// ExitAlter_backup_collection_action is called when production alter_backup_collection_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_backup_collection_action(ctx *Alter_backup_collection_actionContext) {}

// EnterAlter_backup_collection_entries is called when production alter_backup_collection_entries is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_backup_collection_entries(ctx *Alter_backup_collection_entriesContext) {}

// ExitAlter_backup_collection_entries is called when production alter_backup_collection_entries is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_backup_collection_entries(ctx *Alter_backup_collection_entriesContext) {}

// EnterAlter_backup_collection_entry is called when production alter_backup_collection_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_backup_collection_entry(ctx *Alter_backup_collection_entryContext) {}

// ExitAlter_backup_collection_entry is called when production alter_backup_collection_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_backup_collection_entry(ctx *Alter_backup_collection_entryContext) {}

// EnterBackup_collection is called when production backup_collection is entered.
func (s *BaseYQLv1Antlr4Listener) EnterBackup_collection(ctx *Backup_collectionContext) {}

// ExitBackup_collection is called when production backup_collection is exited.
func (s *BaseYQLv1Antlr4Listener) ExitBackup_collection(ctx *Backup_collectionContext) {}

// EnterBackup_collection_settings is called when production backup_collection_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterBackup_collection_settings(ctx *Backup_collection_settingsContext) {}

// ExitBackup_collection_settings is called when production backup_collection_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitBackup_collection_settings(ctx *Backup_collection_settingsContext) {}

// EnterBackup_collection_settings_entry is called when production backup_collection_settings_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterBackup_collection_settings_entry(ctx *Backup_collection_settings_entryContext) {}

// ExitBackup_collection_settings_entry is called when production backup_collection_settings_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitBackup_collection_settings_entry(ctx *Backup_collection_settings_entryContext) {}

// EnterBackup_stmt is called when production backup_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterBackup_stmt(ctx *Backup_stmtContext) {}

// ExitBackup_stmt is called when production backup_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitBackup_stmt(ctx *Backup_stmtContext) {}

// EnterRestore_stmt is called when production restore_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRestore_stmt(ctx *Restore_stmtContext) {}

// ExitRestore_stmt is called when production restore_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRestore_stmt(ctx *Restore_stmtContext) {}

// EnterTable_inherits is called when production table_inherits is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_inherits(ctx *Table_inheritsContext) {}

// ExitTable_inherits is called when production table_inherits is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_inherits(ctx *Table_inheritsContext) {}

// EnterTable_partition_by is called when production table_partition_by is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_partition_by(ctx *Table_partition_byContext) {}

// ExitTable_partition_by is called when production table_partition_by is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_partition_by(ctx *Table_partition_byContext) {}

// EnterWith_table_settings is called when production with_table_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWith_table_settings(ctx *With_table_settingsContext) {}

// ExitWith_table_settings is called when production with_table_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWith_table_settings(ctx *With_table_settingsContext) {}

// EnterTable_tablestore is called when production table_tablestore is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_tablestore(ctx *Table_tablestoreContext) {}

// ExitTable_tablestore is called when production table_tablestore is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_tablestore(ctx *Table_tablestoreContext) {}

// EnterTable_settings_entry is called when production table_settings_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_settings_entry(ctx *Table_settings_entryContext) {}

// ExitTable_settings_entry is called when production table_settings_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_settings_entry(ctx *Table_settings_entryContext) {}

// EnterTable_as_source is called when production table_as_source is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_as_source(ctx *Table_as_sourceContext) {}

// ExitTable_as_source is called when production table_as_source is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_as_source(ctx *Table_as_sourceContext) {}

// EnterAlter_table_stmt is called when production alter_table_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_stmt(ctx *Alter_table_stmtContext) {}

// ExitAlter_table_stmt is called when production alter_table_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_stmt(ctx *Alter_table_stmtContext) {}

// EnterAlter_table_action is called when production alter_table_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_action(ctx *Alter_table_actionContext) {}

// ExitAlter_table_action is called when production alter_table_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_action(ctx *Alter_table_actionContext) {}

// EnterAlter_external_table_stmt is called when production alter_external_table_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_external_table_stmt(ctx *Alter_external_table_stmtContext) {}

// ExitAlter_external_table_stmt is called when production alter_external_table_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_external_table_stmt(ctx *Alter_external_table_stmtContext) {}

// EnterAlter_external_table_action is called when production alter_external_table_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_external_table_action(ctx *Alter_external_table_actionContext) {}

// ExitAlter_external_table_action is called when production alter_external_table_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_external_table_action(ctx *Alter_external_table_actionContext) {}

// EnterAlter_table_store_stmt is called when production alter_table_store_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_store_stmt(ctx *Alter_table_store_stmtContext) {}

// ExitAlter_table_store_stmt is called when production alter_table_store_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_store_stmt(ctx *Alter_table_store_stmtContext) {}

// EnterAlter_table_store_action is called when production alter_table_store_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_store_action(ctx *Alter_table_store_actionContext) {}

// ExitAlter_table_store_action is called when production alter_table_store_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_store_action(ctx *Alter_table_store_actionContext) {}

// EnterAlter_table_add_column is called when production alter_table_add_column is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_add_column(ctx *Alter_table_add_columnContext) {}

// ExitAlter_table_add_column is called when production alter_table_add_column is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_add_column(ctx *Alter_table_add_columnContext) {}

// EnterAlter_table_drop_column is called when production alter_table_drop_column is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_drop_column(ctx *Alter_table_drop_columnContext) {}

// ExitAlter_table_drop_column is called when production alter_table_drop_column is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_drop_column(ctx *Alter_table_drop_columnContext) {}

// EnterAlter_table_alter_column is called when production alter_table_alter_column is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_alter_column(ctx *Alter_table_alter_columnContext) {}

// ExitAlter_table_alter_column is called when production alter_table_alter_column is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_alter_column(ctx *Alter_table_alter_columnContext) {}

// EnterAlter_table_alter_column_drop_not_null is called when production alter_table_alter_column_drop_not_null is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_alter_column_drop_not_null(ctx *Alter_table_alter_column_drop_not_nullContext) {}

// ExitAlter_table_alter_column_drop_not_null is called when production alter_table_alter_column_drop_not_null is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_alter_column_drop_not_null(ctx *Alter_table_alter_column_drop_not_nullContext) {}

// EnterAlter_table_add_column_family is called when production alter_table_add_column_family is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_add_column_family(ctx *Alter_table_add_column_familyContext) {}

// ExitAlter_table_add_column_family is called when production alter_table_add_column_family is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_add_column_family(ctx *Alter_table_add_column_familyContext) {}

// EnterAlter_table_alter_column_family is called when production alter_table_alter_column_family is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_alter_column_family(ctx *Alter_table_alter_column_familyContext) {}

// ExitAlter_table_alter_column_family is called when production alter_table_alter_column_family is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_alter_column_family(ctx *Alter_table_alter_column_familyContext) {}

// EnterAlter_table_set_table_setting_uncompat is called when production alter_table_set_table_setting_uncompat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_set_table_setting_uncompat(ctx *Alter_table_set_table_setting_uncompatContext) {}

// ExitAlter_table_set_table_setting_uncompat is called when production alter_table_set_table_setting_uncompat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_set_table_setting_uncompat(ctx *Alter_table_set_table_setting_uncompatContext) {}

// EnterAlter_table_set_table_setting_compat is called when production alter_table_set_table_setting_compat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_set_table_setting_compat(ctx *Alter_table_set_table_setting_compatContext) {}

// ExitAlter_table_set_table_setting_compat is called when production alter_table_set_table_setting_compat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_set_table_setting_compat(ctx *Alter_table_set_table_setting_compatContext) {}

// EnterAlter_table_reset_table_setting is called when production alter_table_reset_table_setting is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_reset_table_setting(ctx *Alter_table_reset_table_settingContext) {}

// ExitAlter_table_reset_table_setting is called when production alter_table_reset_table_setting is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_reset_table_setting(ctx *Alter_table_reset_table_settingContext) {}

// EnterAlter_table_add_index is called when production alter_table_add_index is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_add_index(ctx *Alter_table_add_indexContext) {}

// ExitAlter_table_add_index is called when production alter_table_add_index is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_add_index(ctx *Alter_table_add_indexContext) {}

// EnterAlter_table_drop_index is called when production alter_table_drop_index is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_drop_index(ctx *Alter_table_drop_indexContext) {}

// ExitAlter_table_drop_index is called when production alter_table_drop_index is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_drop_index(ctx *Alter_table_drop_indexContext) {}

// EnterAlter_table_rename_to is called when production alter_table_rename_to is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_rename_to(ctx *Alter_table_rename_toContext) {}

// ExitAlter_table_rename_to is called when production alter_table_rename_to is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_rename_to(ctx *Alter_table_rename_toContext) {}

// EnterAlter_table_rename_index_to is called when production alter_table_rename_index_to is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_rename_index_to(ctx *Alter_table_rename_index_toContext) {}

// ExitAlter_table_rename_index_to is called when production alter_table_rename_index_to is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_rename_index_to(ctx *Alter_table_rename_index_toContext) {}

// EnterAlter_table_add_changefeed is called when production alter_table_add_changefeed is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_add_changefeed(ctx *Alter_table_add_changefeedContext) {}

// ExitAlter_table_add_changefeed is called when production alter_table_add_changefeed is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_add_changefeed(ctx *Alter_table_add_changefeedContext) {}

// EnterAlter_table_alter_changefeed is called when production alter_table_alter_changefeed is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_alter_changefeed(ctx *Alter_table_alter_changefeedContext) {}

// ExitAlter_table_alter_changefeed is called when production alter_table_alter_changefeed is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_alter_changefeed(ctx *Alter_table_alter_changefeedContext) {}

// EnterAlter_table_drop_changefeed is called when production alter_table_drop_changefeed is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_drop_changefeed(ctx *Alter_table_drop_changefeedContext) {}

// ExitAlter_table_drop_changefeed is called when production alter_table_drop_changefeed is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_drop_changefeed(ctx *Alter_table_drop_changefeedContext) {}

// EnterAlter_table_alter_index is called when production alter_table_alter_index is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_alter_index(ctx *Alter_table_alter_indexContext) {}

// ExitAlter_table_alter_index is called when production alter_table_alter_index is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_alter_index(ctx *Alter_table_alter_indexContext) {}

// EnterColumn_schema is called when production column_schema is entered.
func (s *BaseYQLv1Antlr4Listener) EnterColumn_schema(ctx *Column_schemaContext) {}

// ExitColumn_schema is called when production column_schema is exited.
func (s *BaseYQLv1Antlr4Listener) ExitColumn_schema(ctx *Column_schemaContext) {}

// EnterFamily_relation is called when production family_relation is entered.
func (s *BaseYQLv1Antlr4Listener) EnterFamily_relation(ctx *Family_relationContext) {}

// ExitFamily_relation is called when production family_relation is exited.
func (s *BaseYQLv1Antlr4Listener) ExitFamily_relation(ctx *Family_relationContext) {}

// EnterOpt_column_constraints is called when production opt_column_constraints is entered.
func (s *BaseYQLv1Antlr4Listener) EnterOpt_column_constraints(ctx *Opt_column_constraintsContext) {}

// ExitOpt_column_constraints is called when production opt_column_constraints is exited.
func (s *BaseYQLv1Antlr4Listener) ExitOpt_column_constraints(ctx *Opt_column_constraintsContext) {}

// EnterColumn_order_by_specification is called when production column_order_by_specification is entered.
func (s *BaseYQLv1Antlr4Listener) EnterColumn_order_by_specification(ctx *Column_order_by_specificationContext) {}

// ExitColumn_order_by_specification is called when production column_order_by_specification is exited.
func (s *BaseYQLv1Antlr4Listener) ExitColumn_order_by_specification(ctx *Column_order_by_specificationContext) {}

// EnterTable_constraint is called when production table_constraint is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_constraint(ctx *Table_constraintContext) {}

// ExitTable_constraint is called when production table_constraint is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_constraint(ctx *Table_constraintContext) {}

// EnterTable_index is called when production table_index is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_index(ctx *Table_indexContext) {}

// ExitTable_index is called when production table_index is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_index(ctx *Table_indexContext) {}

// EnterTable_index_type is called when production table_index_type is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_index_type(ctx *Table_index_typeContext) {}

// ExitTable_index_type is called when production table_index_type is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_index_type(ctx *Table_index_typeContext) {}

// EnterGlobal_index is called when production global_index is entered.
func (s *BaseYQLv1Antlr4Listener) EnterGlobal_index(ctx *Global_indexContext) {}

// ExitGlobal_index is called when production global_index is exited.
func (s *BaseYQLv1Antlr4Listener) ExitGlobal_index(ctx *Global_indexContext) {}

// EnterLocal_index is called when production local_index is entered.
func (s *BaseYQLv1Antlr4Listener) EnterLocal_index(ctx *Local_indexContext) {}

// ExitLocal_index is called when production local_index is exited.
func (s *BaseYQLv1Antlr4Listener) ExitLocal_index(ctx *Local_indexContext) {}

// EnterIndex_subtype is called when production index_subtype is entered.
func (s *BaseYQLv1Antlr4Listener) EnterIndex_subtype(ctx *Index_subtypeContext) {}

// ExitIndex_subtype is called when production index_subtype is exited.
func (s *BaseYQLv1Antlr4Listener) ExitIndex_subtype(ctx *Index_subtypeContext) {}

// EnterWith_index_settings is called when production with_index_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWith_index_settings(ctx *With_index_settingsContext) {}

// ExitWith_index_settings is called when production with_index_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWith_index_settings(ctx *With_index_settingsContext) {}

// EnterIndex_setting_entry is called when production index_setting_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterIndex_setting_entry(ctx *Index_setting_entryContext) {}

// ExitIndex_setting_entry is called when production index_setting_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitIndex_setting_entry(ctx *Index_setting_entryContext) {}

// EnterIndex_setting_value is called when production index_setting_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterIndex_setting_value(ctx *Index_setting_valueContext) {}

// ExitIndex_setting_value is called when production index_setting_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitIndex_setting_value(ctx *Index_setting_valueContext) {}

// EnterChangefeed is called when production changefeed is entered.
func (s *BaseYQLv1Antlr4Listener) EnterChangefeed(ctx *ChangefeedContext) {}

// ExitChangefeed is called when production changefeed is exited.
func (s *BaseYQLv1Antlr4Listener) ExitChangefeed(ctx *ChangefeedContext) {}

// EnterChangefeed_settings is called when production changefeed_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterChangefeed_settings(ctx *Changefeed_settingsContext) {}

// ExitChangefeed_settings is called when production changefeed_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitChangefeed_settings(ctx *Changefeed_settingsContext) {}

// EnterChangefeed_settings_entry is called when production changefeed_settings_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterChangefeed_settings_entry(ctx *Changefeed_settings_entryContext) {}

// ExitChangefeed_settings_entry is called when production changefeed_settings_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitChangefeed_settings_entry(ctx *Changefeed_settings_entryContext) {}

// EnterChangefeed_setting_value is called when production changefeed_setting_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterChangefeed_setting_value(ctx *Changefeed_setting_valueContext) {}

// ExitChangefeed_setting_value is called when production changefeed_setting_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitChangefeed_setting_value(ctx *Changefeed_setting_valueContext) {}

// EnterChangefeed_alter_settings is called when production changefeed_alter_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterChangefeed_alter_settings(ctx *Changefeed_alter_settingsContext) {}

// ExitChangefeed_alter_settings is called when production changefeed_alter_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitChangefeed_alter_settings(ctx *Changefeed_alter_settingsContext) {}

// EnterAlter_table_setting_entry is called when production alter_table_setting_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_setting_entry(ctx *Alter_table_setting_entryContext) {}

// ExitAlter_table_setting_entry is called when production alter_table_setting_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_setting_entry(ctx *Alter_table_setting_entryContext) {}

// EnterTable_setting_value is called when production table_setting_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_setting_value(ctx *Table_setting_valueContext) {}

// ExitTable_setting_value is called when production table_setting_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_setting_value(ctx *Table_setting_valueContext) {}

// EnterTtl_tier_list is called when production ttl_tier_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTtl_tier_list(ctx *Ttl_tier_listContext) {}

// ExitTtl_tier_list is called when production ttl_tier_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTtl_tier_list(ctx *Ttl_tier_listContext) {}

// EnterTtl_tier_action is called when production ttl_tier_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTtl_tier_action(ctx *Ttl_tier_actionContext) {}

// ExitTtl_tier_action is called when production ttl_tier_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTtl_tier_action(ctx *Ttl_tier_actionContext) {}

// EnterFamily_entry is called when production family_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterFamily_entry(ctx *Family_entryContext) {}

// ExitFamily_entry is called when production family_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitFamily_entry(ctx *Family_entryContext) {}

// EnterFamily_settings is called when production family_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterFamily_settings(ctx *Family_settingsContext) {}

// ExitFamily_settings is called when production family_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitFamily_settings(ctx *Family_settingsContext) {}

// EnterFamily_settings_entry is called when production family_settings_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterFamily_settings_entry(ctx *Family_settings_entryContext) {}

// ExitFamily_settings_entry is called when production family_settings_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitFamily_settings_entry(ctx *Family_settings_entryContext) {}

// EnterFamily_setting_value is called when production family_setting_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterFamily_setting_value(ctx *Family_setting_valueContext) {}

// ExitFamily_setting_value is called when production family_setting_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitFamily_setting_value(ctx *Family_setting_valueContext) {}

// EnterSplit_boundaries is called when production split_boundaries is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSplit_boundaries(ctx *Split_boundariesContext) {}

// ExitSplit_boundaries is called when production split_boundaries is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSplit_boundaries(ctx *Split_boundariesContext) {}

// EnterLiteral_value_list is called when production literal_value_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterLiteral_value_list(ctx *Literal_value_listContext) {}

// ExitLiteral_value_list is called when production literal_value_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitLiteral_value_list(ctx *Literal_value_listContext) {}

// EnterAlter_table_alter_index_action is called when production alter_table_alter_index_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_table_alter_index_action(ctx *Alter_table_alter_index_actionContext) {}

// ExitAlter_table_alter_index_action is called when production alter_table_alter_index_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_table_alter_index_action(ctx *Alter_table_alter_index_actionContext) {}

// EnterDrop_table_stmt is called when production drop_table_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_table_stmt(ctx *Drop_table_stmtContext) {}

// ExitDrop_table_stmt is called when production drop_table_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_table_stmt(ctx *Drop_table_stmtContext) {}

// EnterCreate_user_stmt is called when production create_user_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_user_stmt(ctx *Create_user_stmtContext) {}

// ExitCreate_user_stmt is called when production create_user_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_user_stmt(ctx *Create_user_stmtContext) {}

// EnterAlter_user_stmt is called when production alter_user_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_user_stmt(ctx *Alter_user_stmtContext) {}

// ExitAlter_user_stmt is called when production alter_user_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_user_stmt(ctx *Alter_user_stmtContext) {}

// EnterCreate_group_stmt is called when production create_group_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_group_stmt(ctx *Create_group_stmtContext) {}

// ExitCreate_group_stmt is called when production create_group_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_group_stmt(ctx *Create_group_stmtContext) {}

// EnterAlter_group_stmt is called when production alter_group_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_group_stmt(ctx *Alter_group_stmtContext) {}

// ExitAlter_group_stmt is called when production alter_group_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_group_stmt(ctx *Alter_group_stmtContext) {}

// EnterDrop_role_stmt is called when production drop_role_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_role_stmt(ctx *Drop_role_stmtContext) {}

// ExitDrop_role_stmt is called when production drop_role_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_role_stmt(ctx *Drop_role_stmtContext) {}

// EnterRole_name is called when production role_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRole_name(ctx *Role_nameContext) {}

// ExitRole_name is called when production role_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRole_name(ctx *Role_nameContext) {}

// EnterCreate_user_option is called when production create_user_option is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_user_option(ctx *Create_user_optionContext) {}

// ExitCreate_user_option is called when production create_user_option is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_user_option(ctx *Create_user_optionContext) {}

// EnterPassword_option is called when production password_option is entered.
func (s *BaseYQLv1Antlr4Listener) EnterPassword_option(ctx *Password_optionContext) {}

// ExitPassword_option is called when production password_option is exited.
func (s *BaseYQLv1Antlr4Listener) ExitPassword_option(ctx *Password_optionContext) {}

// EnterLogin_option is called when production login_option is entered.
func (s *BaseYQLv1Antlr4Listener) EnterLogin_option(ctx *Login_optionContext) {}

// ExitLogin_option is called when production login_option is exited.
func (s *BaseYQLv1Antlr4Listener) ExitLogin_option(ctx *Login_optionContext) {}

// EnterGrant_permissions_stmt is called when production grant_permissions_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterGrant_permissions_stmt(ctx *Grant_permissions_stmtContext) {}

// ExitGrant_permissions_stmt is called when production grant_permissions_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitGrant_permissions_stmt(ctx *Grant_permissions_stmtContext) {}

// EnterRevoke_permissions_stmt is called when production revoke_permissions_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRevoke_permissions_stmt(ctx *Revoke_permissions_stmtContext) {}

// ExitRevoke_permissions_stmt is called when production revoke_permissions_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRevoke_permissions_stmt(ctx *Revoke_permissions_stmtContext) {}

// EnterPermission_id is called when production permission_id is entered.
func (s *BaseYQLv1Antlr4Listener) EnterPermission_id(ctx *Permission_idContext) {}

// ExitPermission_id is called when production permission_id is exited.
func (s *BaseYQLv1Antlr4Listener) ExitPermission_id(ctx *Permission_idContext) {}

// EnterPermission_name is called when production permission_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterPermission_name(ctx *Permission_nameContext) {}

// ExitPermission_name is called when production permission_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitPermission_name(ctx *Permission_nameContext) {}

// EnterPermission_name_target is called when production permission_name_target is entered.
func (s *BaseYQLv1Antlr4Listener) EnterPermission_name_target(ctx *Permission_name_targetContext) {}

// ExitPermission_name_target is called when production permission_name_target is exited.
func (s *BaseYQLv1Antlr4Listener) ExitPermission_name_target(ctx *Permission_name_targetContext) {}

// EnterCreate_resource_pool_stmt is called when production create_resource_pool_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_resource_pool_stmt(ctx *Create_resource_pool_stmtContext) {}

// ExitCreate_resource_pool_stmt is called when production create_resource_pool_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_resource_pool_stmt(ctx *Create_resource_pool_stmtContext) {}

// EnterAlter_resource_pool_stmt is called when production alter_resource_pool_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_resource_pool_stmt(ctx *Alter_resource_pool_stmtContext) {}

// ExitAlter_resource_pool_stmt is called when production alter_resource_pool_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_resource_pool_stmt(ctx *Alter_resource_pool_stmtContext) {}

// EnterAlter_resource_pool_action is called when production alter_resource_pool_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_resource_pool_action(ctx *Alter_resource_pool_actionContext) {}

// ExitAlter_resource_pool_action is called when production alter_resource_pool_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_resource_pool_action(ctx *Alter_resource_pool_actionContext) {}

// EnterDrop_resource_pool_stmt is called when production drop_resource_pool_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_resource_pool_stmt(ctx *Drop_resource_pool_stmtContext) {}

// ExitDrop_resource_pool_stmt is called when production drop_resource_pool_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_resource_pool_stmt(ctx *Drop_resource_pool_stmtContext) {}

// EnterCreate_resource_pool_classifier_stmt is called when production create_resource_pool_classifier_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_resource_pool_classifier_stmt(ctx *Create_resource_pool_classifier_stmtContext) {}

// ExitCreate_resource_pool_classifier_stmt is called when production create_resource_pool_classifier_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_resource_pool_classifier_stmt(ctx *Create_resource_pool_classifier_stmtContext) {}

// EnterAlter_resource_pool_classifier_stmt is called when production alter_resource_pool_classifier_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_resource_pool_classifier_stmt(ctx *Alter_resource_pool_classifier_stmtContext) {}

// ExitAlter_resource_pool_classifier_stmt is called when production alter_resource_pool_classifier_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_resource_pool_classifier_stmt(ctx *Alter_resource_pool_classifier_stmtContext) {}

// EnterAlter_resource_pool_classifier_action is called when production alter_resource_pool_classifier_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_resource_pool_classifier_action(ctx *Alter_resource_pool_classifier_actionContext) {}

// ExitAlter_resource_pool_classifier_action is called when production alter_resource_pool_classifier_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_resource_pool_classifier_action(ctx *Alter_resource_pool_classifier_actionContext) {}

// EnterDrop_resource_pool_classifier_stmt is called when production drop_resource_pool_classifier_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_resource_pool_classifier_stmt(ctx *Drop_resource_pool_classifier_stmtContext) {}

// ExitDrop_resource_pool_classifier_stmt is called when production drop_resource_pool_classifier_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_resource_pool_classifier_stmt(ctx *Drop_resource_pool_classifier_stmtContext) {}

// EnterCreate_replication_stmt is called when production create_replication_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_replication_stmt(ctx *Create_replication_stmtContext) {}

// ExitCreate_replication_stmt is called when production create_replication_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_replication_stmt(ctx *Create_replication_stmtContext) {}

// EnterReplication_target is called when production replication_target is entered.
func (s *BaseYQLv1Antlr4Listener) EnterReplication_target(ctx *Replication_targetContext) {}

// ExitReplication_target is called when production replication_target is exited.
func (s *BaseYQLv1Antlr4Listener) ExitReplication_target(ctx *Replication_targetContext) {}

// EnterReplication_settings is called when production replication_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterReplication_settings(ctx *Replication_settingsContext) {}

// ExitReplication_settings is called when production replication_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitReplication_settings(ctx *Replication_settingsContext) {}

// EnterReplication_settings_entry is called when production replication_settings_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterReplication_settings_entry(ctx *Replication_settings_entryContext) {}

// ExitReplication_settings_entry is called when production replication_settings_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitReplication_settings_entry(ctx *Replication_settings_entryContext) {}

// EnterAlter_replication_stmt is called when production alter_replication_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_replication_stmt(ctx *Alter_replication_stmtContext) {}

// ExitAlter_replication_stmt is called when production alter_replication_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_replication_stmt(ctx *Alter_replication_stmtContext) {}

// EnterAlter_replication_action is called when production alter_replication_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_replication_action(ctx *Alter_replication_actionContext) {}

// ExitAlter_replication_action is called when production alter_replication_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_replication_action(ctx *Alter_replication_actionContext) {}

// EnterAlter_replication_set_setting is called when production alter_replication_set_setting is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_replication_set_setting(ctx *Alter_replication_set_settingContext) {}

// ExitAlter_replication_set_setting is called when production alter_replication_set_setting is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_replication_set_setting(ctx *Alter_replication_set_settingContext) {}

// EnterDrop_replication_stmt is called when production drop_replication_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_replication_stmt(ctx *Drop_replication_stmtContext) {}

// ExitDrop_replication_stmt is called when production drop_replication_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_replication_stmt(ctx *Drop_replication_stmtContext) {}

// EnterAction_or_subquery_args is called when production action_or_subquery_args is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAction_or_subquery_args(ctx *Action_or_subquery_argsContext) {}

// ExitAction_or_subquery_args is called when production action_or_subquery_args is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAction_or_subquery_args(ctx *Action_or_subquery_argsContext) {}

// EnterDefine_action_or_subquery_stmt is called when production define_action_or_subquery_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDefine_action_or_subquery_stmt(ctx *Define_action_or_subquery_stmtContext) {}

// ExitDefine_action_or_subquery_stmt is called when production define_action_or_subquery_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDefine_action_or_subquery_stmt(ctx *Define_action_or_subquery_stmtContext) {}

// EnterDefine_action_or_subquery_body is called when production define_action_or_subquery_body is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDefine_action_or_subquery_body(ctx *Define_action_or_subquery_bodyContext) {}

// ExitDefine_action_or_subquery_body is called when production define_action_or_subquery_body is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDefine_action_or_subquery_body(ctx *Define_action_or_subquery_bodyContext) {}

// EnterIf_stmt is called when production if_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterIf_stmt(ctx *If_stmtContext) {}

// ExitIf_stmt is called when production if_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitIf_stmt(ctx *If_stmtContext) {}

// EnterFor_stmt is called when production for_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterFor_stmt(ctx *For_stmtContext) {}

// ExitFor_stmt is called when production for_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitFor_stmt(ctx *For_stmtContext) {}

// EnterTable_ref is called when production table_ref is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_ref(ctx *Table_refContext) {}

// ExitTable_ref is called when production table_ref is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_ref(ctx *Table_refContext) {}

// EnterTable_key is called when production table_key is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_key(ctx *Table_keyContext) {}

// ExitTable_key is called when production table_key is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_key(ctx *Table_keyContext) {}

// EnterTable_arg is called when production table_arg is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_arg(ctx *Table_argContext) {}

// ExitTable_arg is called when production table_arg is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_arg(ctx *Table_argContext) {}

// EnterTable_hints is called when production table_hints is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_hints(ctx *Table_hintsContext) {}

// ExitTable_hints is called when production table_hints is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_hints(ctx *Table_hintsContext) {}

// EnterTable_hint is called when production table_hint is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTable_hint(ctx *Table_hintContext) {}

// ExitTable_hint is called when production table_hint is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTable_hint(ctx *Table_hintContext) {}

// EnterObject_ref is called when production object_ref is entered.
func (s *BaseYQLv1Antlr4Listener) EnterObject_ref(ctx *Object_refContext) {}

// ExitObject_ref is called when production object_ref is exited.
func (s *BaseYQLv1Antlr4Listener) ExitObject_ref(ctx *Object_refContext) {}

// EnterSimple_table_ref_core is called when production simple_table_ref_core is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSimple_table_ref_core(ctx *Simple_table_ref_coreContext) {}

// ExitSimple_table_ref_core is called when production simple_table_ref_core is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSimple_table_ref_core(ctx *Simple_table_ref_coreContext) {}

// EnterSimple_table_ref is called when production simple_table_ref is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSimple_table_ref(ctx *Simple_table_refContext) {}

// ExitSimple_table_ref is called when production simple_table_ref is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSimple_table_ref(ctx *Simple_table_refContext) {}

// EnterInto_simple_table_ref is called when production into_simple_table_ref is entered.
func (s *BaseYQLv1Antlr4Listener) EnterInto_simple_table_ref(ctx *Into_simple_table_refContext) {}

// ExitInto_simple_table_ref is called when production into_simple_table_ref is exited.
func (s *BaseYQLv1Antlr4Listener) ExitInto_simple_table_ref(ctx *Into_simple_table_refContext) {}

// EnterDelete_stmt is called when production delete_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDelete_stmt(ctx *Delete_stmtContext) {}

// ExitDelete_stmt is called when production delete_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDelete_stmt(ctx *Delete_stmtContext) {}

// EnterUpdate_stmt is called when production update_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterUpdate_stmt(ctx *Update_stmtContext) {}

// ExitUpdate_stmt is called when production update_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitUpdate_stmt(ctx *Update_stmtContext) {}

// EnterSet_clause_choice is called when production set_clause_choice is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSet_clause_choice(ctx *Set_clause_choiceContext) {}

// ExitSet_clause_choice is called when production set_clause_choice is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSet_clause_choice(ctx *Set_clause_choiceContext) {}

// EnterSet_clause_list is called when production set_clause_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSet_clause_list(ctx *Set_clause_listContext) {}

// ExitSet_clause_list is called when production set_clause_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSet_clause_list(ctx *Set_clause_listContext) {}

// EnterSet_clause is called when production set_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSet_clause(ctx *Set_clauseContext) {}

// ExitSet_clause is called when production set_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSet_clause(ctx *Set_clauseContext) {}

// EnterSet_target is called when production set_target is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSet_target(ctx *Set_targetContext) {}

// ExitSet_target is called when production set_target is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSet_target(ctx *Set_targetContext) {}

// EnterMultiple_column_assignment is called when production multiple_column_assignment is entered.
func (s *BaseYQLv1Antlr4Listener) EnterMultiple_column_assignment(ctx *Multiple_column_assignmentContext) {}

// ExitMultiple_column_assignment is called when production multiple_column_assignment is exited.
func (s *BaseYQLv1Antlr4Listener) ExitMultiple_column_assignment(ctx *Multiple_column_assignmentContext) {}

// EnterSet_target_list is called when production set_target_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSet_target_list(ctx *Set_target_listContext) {}

// ExitSet_target_list is called when production set_target_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSet_target_list(ctx *Set_target_listContext) {}

// EnterCreate_topic_stmt is called when production create_topic_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_topic_stmt(ctx *Create_topic_stmtContext) {}

// ExitCreate_topic_stmt is called when production create_topic_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_topic_stmt(ctx *Create_topic_stmtContext) {}

// EnterCreate_topic_entries is called when production create_topic_entries is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_topic_entries(ctx *Create_topic_entriesContext) {}

// ExitCreate_topic_entries is called when production create_topic_entries is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_topic_entries(ctx *Create_topic_entriesContext) {}

// EnterCreate_topic_entry is called when production create_topic_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCreate_topic_entry(ctx *Create_topic_entryContext) {}

// ExitCreate_topic_entry is called when production create_topic_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCreate_topic_entry(ctx *Create_topic_entryContext) {}

// EnterWith_topic_settings is called when production with_topic_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWith_topic_settings(ctx *With_topic_settingsContext) {}

// ExitWith_topic_settings is called when production with_topic_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWith_topic_settings(ctx *With_topic_settingsContext) {}

// EnterAlter_topic_stmt is called when production alter_topic_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_topic_stmt(ctx *Alter_topic_stmtContext) {}

// ExitAlter_topic_stmt is called when production alter_topic_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_topic_stmt(ctx *Alter_topic_stmtContext) {}

// EnterAlter_topic_action is called when production alter_topic_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_topic_action(ctx *Alter_topic_actionContext) {}

// ExitAlter_topic_action is called when production alter_topic_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_topic_action(ctx *Alter_topic_actionContext) {}

// EnterAlter_topic_add_consumer is called when production alter_topic_add_consumer is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_topic_add_consumer(ctx *Alter_topic_add_consumerContext) {}

// ExitAlter_topic_add_consumer is called when production alter_topic_add_consumer is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_topic_add_consumer(ctx *Alter_topic_add_consumerContext) {}

// EnterTopic_create_consumer_entry is called when production topic_create_consumer_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_create_consumer_entry(ctx *Topic_create_consumer_entryContext) {}

// ExitTopic_create_consumer_entry is called when production topic_create_consumer_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_create_consumer_entry(ctx *Topic_create_consumer_entryContext) {}

// EnterAlter_topic_alter_consumer is called when production alter_topic_alter_consumer is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_topic_alter_consumer(ctx *Alter_topic_alter_consumerContext) {}

// ExitAlter_topic_alter_consumer is called when production alter_topic_alter_consumer is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_topic_alter_consumer(ctx *Alter_topic_alter_consumerContext) {}

// EnterAlter_topic_alter_consumer_entry is called when production alter_topic_alter_consumer_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_topic_alter_consumer_entry(ctx *Alter_topic_alter_consumer_entryContext) {}

// ExitAlter_topic_alter_consumer_entry is called when production alter_topic_alter_consumer_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_topic_alter_consumer_entry(ctx *Alter_topic_alter_consumer_entryContext) {}

// EnterAlter_topic_drop_consumer is called when production alter_topic_drop_consumer is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_topic_drop_consumer(ctx *Alter_topic_drop_consumerContext) {}

// ExitAlter_topic_drop_consumer is called when production alter_topic_drop_consumer is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_topic_drop_consumer(ctx *Alter_topic_drop_consumerContext) {}

// EnterTopic_alter_consumer_set is called when production topic_alter_consumer_set is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_alter_consumer_set(ctx *Topic_alter_consumer_setContext) {}

// ExitTopic_alter_consumer_set is called when production topic_alter_consumer_set is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_alter_consumer_set(ctx *Topic_alter_consumer_setContext) {}

// EnterTopic_alter_consumer_reset is called when production topic_alter_consumer_reset is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_alter_consumer_reset(ctx *Topic_alter_consumer_resetContext) {}

// ExitTopic_alter_consumer_reset is called when production topic_alter_consumer_reset is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_alter_consumer_reset(ctx *Topic_alter_consumer_resetContext) {}

// EnterAlter_topic_set_settings is called when production alter_topic_set_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_topic_set_settings(ctx *Alter_topic_set_settingsContext) {}

// ExitAlter_topic_set_settings is called when production alter_topic_set_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_topic_set_settings(ctx *Alter_topic_set_settingsContext) {}

// EnterAlter_topic_reset_settings is called when production alter_topic_reset_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_topic_reset_settings(ctx *Alter_topic_reset_settingsContext) {}

// ExitAlter_topic_reset_settings is called when production alter_topic_reset_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_topic_reset_settings(ctx *Alter_topic_reset_settingsContext) {}

// EnterDrop_topic_stmt is called when production drop_topic_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterDrop_topic_stmt(ctx *Drop_topic_stmtContext) {}

// ExitDrop_topic_stmt is called when production drop_topic_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitDrop_topic_stmt(ctx *Drop_topic_stmtContext) {}

// EnterTopic_settings is called when production topic_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_settings(ctx *Topic_settingsContext) {}

// ExitTopic_settings is called when production topic_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_settings(ctx *Topic_settingsContext) {}

// EnterTopic_settings_entry is called when production topic_settings_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_settings_entry(ctx *Topic_settings_entryContext) {}

// ExitTopic_settings_entry is called when production topic_settings_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_settings_entry(ctx *Topic_settings_entryContext) {}

// EnterTopic_setting_value is called when production topic_setting_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_setting_value(ctx *Topic_setting_valueContext) {}

// ExitTopic_setting_value is called when production topic_setting_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_setting_value(ctx *Topic_setting_valueContext) {}

// EnterTopic_consumer_with_settings is called when production topic_consumer_with_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_consumer_with_settings(ctx *Topic_consumer_with_settingsContext) {}

// ExitTopic_consumer_with_settings is called when production topic_consumer_with_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_consumer_with_settings(ctx *Topic_consumer_with_settingsContext) {}

// EnterTopic_consumer_settings is called when production topic_consumer_settings is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_consumer_settings(ctx *Topic_consumer_settingsContext) {}

// ExitTopic_consumer_settings is called when production topic_consumer_settings is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_consumer_settings(ctx *Topic_consumer_settingsContext) {}

// EnterTopic_consumer_settings_entry is called when production topic_consumer_settings_entry is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_consumer_settings_entry(ctx *Topic_consumer_settings_entryContext) {}

// ExitTopic_consumer_settings_entry is called when production topic_consumer_settings_entry is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_consumer_settings_entry(ctx *Topic_consumer_settings_entryContext) {}

// EnterTopic_consumer_setting_value is called when production topic_consumer_setting_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_consumer_setting_value(ctx *Topic_consumer_setting_valueContext) {}

// ExitTopic_consumer_setting_value is called when production topic_consumer_setting_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_consumer_setting_value(ctx *Topic_consumer_setting_valueContext) {}

// EnterTopic_ref is called when production topic_ref is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_ref(ctx *Topic_refContext) {}

// ExitTopic_ref is called when production topic_ref is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_ref(ctx *Topic_refContext) {}

// EnterTopic_consumer_ref is called when production topic_consumer_ref is entered.
func (s *BaseYQLv1Antlr4Listener) EnterTopic_consumer_ref(ctx *Topic_consumer_refContext) {}

// ExitTopic_consumer_ref is called when production topic_consumer_ref is exited.
func (s *BaseYQLv1Antlr4Listener) ExitTopic_consumer_ref(ctx *Topic_consumer_refContext) {}

// EnterNull_treatment is called when production null_treatment is entered.
func (s *BaseYQLv1Antlr4Listener) EnterNull_treatment(ctx *Null_treatmentContext) {}

// ExitNull_treatment is called when production null_treatment is exited.
func (s *BaseYQLv1Antlr4Listener) ExitNull_treatment(ctx *Null_treatmentContext) {}

// EnterFilter_clause is called when production filter_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterFilter_clause(ctx *Filter_clauseContext) {}

// ExitFilter_clause is called when production filter_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitFilter_clause(ctx *Filter_clauseContext) {}

// EnterWindow_name_or_specification is called when production window_name_or_specification is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_name_or_specification(ctx *Window_name_or_specificationContext) {}

// ExitWindow_name_or_specification is called when production window_name_or_specification is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_name_or_specification(ctx *Window_name_or_specificationContext) {}

// EnterWindow_name is called when production window_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_name(ctx *Window_nameContext) {}

// ExitWindow_name is called when production window_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_name(ctx *Window_nameContext) {}

// EnterWindow_clause is called when production window_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_clause(ctx *Window_clauseContext) {}

// ExitWindow_clause is called when production window_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_clause(ctx *Window_clauseContext) {}

// EnterWindow_definition_list is called when production window_definition_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_definition_list(ctx *Window_definition_listContext) {}

// ExitWindow_definition_list is called when production window_definition_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_definition_list(ctx *Window_definition_listContext) {}

// EnterWindow_definition is called when production window_definition is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_definition(ctx *Window_definitionContext) {}

// ExitWindow_definition is called when production window_definition is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_definition(ctx *Window_definitionContext) {}

// EnterNew_window_name is called when production new_window_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterNew_window_name(ctx *New_window_nameContext) {}

// ExitNew_window_name is called when production new_window_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitNew_window_name(ctx *New_window_nameContext) {}

// EnterWindow_specification is called when production window_specification is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_specification(ctx *Window_specificationContext) {}

// ExitWindow_specification is called when production window_specification is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_specification(ctx *Window_specificationContext) {}

// EnterWindow_specification_details is called when production window_specification_details is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_specification_details(ctx *Window_specification_detailsContext) {}

// ExitWindow_specification_details is called when production window_specification_details is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_specification_details(ctx *Window_specification_detailsContext) {}

// EnterExisting_window_name is called when production existing_window_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterExisting_window_name(ctx *Existing_window_nameContext) {}

// ExitExisting_window_name is called when production existing_window_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitExisting_window_name(ctx *Existing_window_nameContext) {}

// EnterWindow_partition_clause is called when production window_partition_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_partition_clause(ctx *Window_partition_clauseContext) {}

// ExitWindow_partition_clause is called when production window_partition_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_partition_clause(ctx *Window_partition_clauseContext) {}

// EnterWindow_order_clause is called when production window_order_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_order_clause(ctx *Window_order_clauseContext) {}

// ExitWindow_order_clause is called when production window_order_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_order_clause(ctx *Window_order_clauseContext) {}

// EnterWindow_frame_clause is called when production window_frame_clause is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_frame_clause(ctx *Window_frame_clauseContext) {}

// ExitWindow_frame_clause is called when production window_frame_clause is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_frame_clause(ctx *Window_frame_clauseContext) {}

// EnterWindow_frame_units is called when production window_frame_units is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_frame_units(ctx *Window_frame_unitsContext) {}

// ExitWindow_frame_units is called when production window_frame_units is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_frame_units(ctx *Window_frame_unitsContext) {}

// EnterWindow_frame_extent is called when production window_frame_extent is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_frame_extent(ctx *Window_frame_extentContext) {}

// ExitWindow_frame_extent is called when production window_frame_extent is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_frame_extent(ctx *Window_frame_extentContext) {}

// EnterWindow_frame_between is called when production window_frame_between is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_frame_between(ctx *Window_frame_betweenContext) {}

// ExitWindow_frame_between is called when production window_frame_between is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_frame_between(ctx *Window_frame_betweenContext) {}

// EnterWindow_frame_bound is called when production window_frame_bound is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_frame_bound(ctx *Window_frame_boundContext) {}

// ExitWindow_frame_bound is called when production window_frame_bound is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_frame_bound(ctx *Window_frame_boundContext) {}

// EnterWindow_frame_exclusion is called when production window_frame_exclusion is entered.
func (s *BaseYQLv1Antlr4Listener) EnterWindow_frame_exclusion(ctx *Window_frame_exclusionContext) {}

// ExitWindow_frame_exclusion is called when production window_frame_exclusion is exited.
func (s *BaseYQLv1Antlr4Listener) ExitWindow_frame_exclusion(ctx *Window_frame_exclusionContext) {}

// EnterUse_stmt is called when production use_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterUse_stmt(ctx *Use_stmtContext) {}

// ExitUse_stmt is called when production use_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitUse_stmt(ctx *Use_stmtContext) {}

// EnterSubselect_stmt is called when production subselect_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterSubselect_stmt(ctx *Subselect_stmtContext) {}

// ExitSubselect_stmt is called when production subselect_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitSubselect_stmt(ctx *Subselect_stmtContext) {}

// EnterNamed_nodes_stmt is called when production named_nodes_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterNamed_nodes_stmt(ctx *Named_nodes_stmtContext) {}

// ExitNamed_nodes_stmt is called when production named_nodes_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitNamed_nodes_stmt(ctx *Named_nodes_stmtContext) {}

// EnterCommit_stmt is called when production commit_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCommit_stmt(ctx *Commit_stmtContext) {}

// ExitCommit_stmt is called when production commit_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCommit_stmt(ctx *Commit_stmtContext) {}

// EnterRollback_stmt is called when production rollback_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterRollback_stmt(ctx *Rollback_stmtContext) {}

// ExitRollback_stmt is called when production rollback_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitRollback_stmt(ctx *Rollback_stmtContext) {}

// EnterAnalyze_table is called when production analyze_table is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAnalyze_table(ctx *Analyze_tableContext) {}

// ExitAnalyze_table is called when production analyze_table is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAnalyze_table(ctx *Analyze_tableContext) {}

// EnterAnalyze_table_list is called when production analyze_table_list is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAnalyze_table_list(ctx *Analyze_table_listContext) {}

// ExitAnalyze_table_list is called when production analyze_table_list is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAnalyze_table_list(ctx *Analyze_table_listContext) {}

// EnterAnalyze_stmt is called when production analyze_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAnalyze_stmt(ctx *Analyze_stmtContext) {}

// ExitAnalyze_stmt is called when production analyze_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAnalyze_stmt(ctx *Analyze_stmtContext) {}

// EnterAlter_sequence_stmt is called when production alter_sequence_stmt is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_sequence_stmt(ctx *Alter_sequence_stmtContext) {}

// ExitAlter_sequence_stmt is called when production alter_sequence_stmt is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_sequence_stmt(ctx *Alter_sequence_stmtContext) {}

// EnterAlter_sequence_action is called when production alter_sequence_action is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAlter_sequence_action(ctx *Alter_sequence_actionContext) {}

// ExitAlter_sequence_action is called when production alter_sequence_action is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAlter_sequence_action(ctx *Alter_sequence_actionContext) {}

// EnterIdentifier is called when production identifier is entered.
func (s *BaseYQLv1Antlr4Listener) EnterIdentifier(ctx *IdentifierContext) {}

// ExitIdentifier is called when production identifier is exited.
func (s *BaseYQLv1Antlr4Listener) ExitIdentifier(ctx *IdentifierContext) {}

// EnterId is called when production id is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId(ctx *IdContext) {}

// ExitId is called when production id is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId(ctx *IdContext) {}

// EnterId_schema is called when production id_schema is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_schema(ctx *Id_schemaContext) {}

// ExitId_schema is called when production id_schema is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_schema(ctx *Id_schemaContext) {}

// EnterId_expr is called when production id_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_expr(ctx *Id_exprContext) {}

// ExitId_expr is called when production id_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_expr(ctx *Id_exprContext) {}

// EnterId_expr_in is called when production id_expr_in is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_expr_in(ctx *Id_expr_inContext) {}

// ExitId_expr_in is called when production id_expr_in is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_expr_in(ctx *Id_expr_inContext) {}

// EnterId_window is called when production id_window is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_window(ctx *Id_windowContext) {}

// ExitId_window is called when production id_window is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_window(ctx *Id_windowContext) {}

// EnterId_table is called when production id_table is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_table(ctx *Id_tableContext) {}

// ExitId_table is called when production id_table is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_table(ctx *Id_tableContext) {}

// EnterId_without is called when production id_without is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_without(ctx *Id_withoutContext) {}

// ExitId_without is called when production id_without is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_without(ctx *Id_withoutContext) {}

// EnterId_hint is called when production id_hint is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_hint(ctx *Id_hintContext) {}

// ExitId_hint is called when production id_hint is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_hint(ctx *Id_hintContext) {}

// EnterId_as_compat is called when production id_as_compat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_as_compat(ctx *Id_as_compatContext) {}

// ExitId_as_compat is called when production id_as_compat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_as_compat(ctx *Id_as_compatContext) {}

// EnterAn_id is called when production an_id is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id(ctx *An_idContext) {}

// ExitAn_id is called when production an_id is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id(ctx *An_idContext) {}

// EnterAn_id_or_type is called when production an_id_or_type is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id_or_type(ctx *An_id_or_typeContext) {}

// ExitAn_id_or_type is called when production an_id_or_type is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id_or_type(ctx *An_id_or_typeContext) {}

// EnterAn_id_schema is called when production an_id_schema is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id_schema(ctx *An_id_schemaContext) {}

// ExitAn_id_schema is called when production an_id_schema is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id_schema(ctx *An_id_schemaContext) {}

// EnterAn_id_expr is called when production an_id_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id_expr(ctx *An_id_exprContext) {}

// ExitAn_id_expr is called when production an_id_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id_expr(ctx *An_id_exprContext) {}

// EnterAn_id_expr_in is called when production an_id_expr_in is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id_expr_in(ctx *An_id_expr_inContext) {}

// ExitAn_id_expr_in is called when production an_id_expr_in is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id_expr_in(ctx *An_id_expr_inContext) {}

// EnterAn_id_window is called when production an_id_window is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id_window(ctx *An_id_windowContext) {}

// ExitAn_id_window is called when production an_id_window is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id_window(ctx *An_id_windowContext) {}

// EnterAn_id_table is called when production an_id_table is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id_table(ctx *An_id_tableContext) {}

// ExitAn_id_table is called when production an_id_table is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id_table(ctx *An_id_tableContext) {}

// EnterAn_id_without is called when production an_id_without is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id_without(ctx *An_id_withoutContext) {}

// ExitAn_id_without is called when production an_id_without is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id_without(ctx *An_id_withoutContext) {}

// EnterAn_id_hint is called when production an_id_hint is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id_hint(ctx *An_id_hintContext) {}

// ExitAn_id_hint is called when production an_id_hint is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id_hint(ctx *An_id_hintContext) {}

// EnterAn_id_pure is called when production an_id_pure is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id_pure(ctx *An_id_pureContext) {}

// ExitAn_id_pure is called when production an_id_pure is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id_pure(ctx *An_id_pureContext) {}

// EnterAn_id_as_compat is called when production an_id_as_compat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterAn_id_as_compat(ctx *An_id_as_compatContext) {}

// ExitAn_id_as_compat is called when production an_id_as_compat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitAn_id_as_compat(ctx *An_id_as_compatContext) {}

// EnterView_name is called when production view_name is entered.
func (s *BaseYQLv1Antlr4Listener) EnterView_name(ctx *View_nameContext) {}

// ExitView_name is called when production view_name is exited.
func (s *BaseYQLv1Antlr4Listener) ExitView_name(ctx *View_nameContext) {}

// EnterOpt_id_prefix is called when production opt_id_prefix is entered.
func (s *BaseYQLv1Antlr4Listener) EnterOpt_id_prefix(ctx *Opt_id_prefixContext) {}

// ExitOpt_id_prefix is called when production opt_id_prefix is exited.
func (s *BaseYQLv1Antlr4Listener) ExitOpt_id_prefix(ctx *Opt_id_prefixContext) {}

// EnterCluster_expr is called when production cluster_expr is entered.
func (s *BaseYQLv1Antlr4Listener) EnterCluster_expr(ctx *Cluster_exprContext) {}

// ExitCluster_expr is called when production cluster_expr is exited.
func (s *BaseYQLv1Antlr4Listener) ExitCluster_expr(ctx *Cluster_exprContext) {}

// EnterId_or_type is called when production id_or_type is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_or_type(ctx *Id_or_typeContext) {}

// ExitId_or_type is called when production id_or_type is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_or_type(ctx *Id_or_typeContext) {}

// EnterOpt_id_prefix_or_type is called when production opt_id_prefix_or_type is entered.
func (s *BaseYQLv1Antlr4Listener) EnterOpt_id_prefix_or_type(ctx *Opt_id_prefix_or_typeContext) {}

// ExitOpt_id_prefix_or_type is called when production opt_id_prefix_or_type is exited.
func (s *BaseYQLv1Antlr4Listener) ExitOpt_id_prefix_or_type(ctx *Opt_id_prefix_or_typeContext) {}

// EnterId_or_at is called when production id_or_at is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_or_at(ctx *Id_or_atContext) {}

// ExitId_or_at is called when production id_or_at is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_or_at(ctx *Id_or_atContext) {}

// EnterId_table_or_type is called when production id_table_or_type is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_table_or_type(ctx *Id_table_or_typeContext) {}

// ExitId_table_or_type is called when production id_table_or_type is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_table_or_type(ctx *Id_table_or_typeContext) {}

// EnterId_table_or_at is called when production id_table_or_at is entered.
func (s *BaseYQLv1Antlr4Listener) EnterId_table_or_at(ctx *Id_table_or_atContext) {}

// ExitId_table_or_at is called when production id_table_or_at is exited.
func (s *BaseYQLv1Antlr4Listener) ExitId_table_or_at(ctx *Id_table_or_atContext) {}

// EnterKeyword is called when production keyword is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKeyword(ctx *KeywordContext) {}

// ExitKeyword is called when production keyword is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKeyword(ctx *KeywordContext) {}

// EnterKeyword_expr_uncompat is called when production keyword_expr_uncompat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKeyword_expr_uncompat(ctx *Keyword_expr_uncompatContext) {}

// ExitKeyword_expr_uncompat is called when production keyword_expr_uncompat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKeyword_expr_uncompat(ctx *Keyword_expr_uncompatContext) {}

// EnterKeyword_table_uncompat is called when production keyword_table_uncompat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKeyword_table_uncompat(ctx *Keyword_table_uncompatContext) {}

// ExitKeyword_table_uncompat is called when production keyword_table_uncompat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKeyword_table_uncompat(ctx *Keyword_table_uncompatContext) {}

// EnterKeyword_select_uncompat is called when production keyword_select_uncompat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKeyword_select_uncompat(ctx *Keyword_select_uncompatContext) {}

// ExitKeyword_select_uncompat is called when production keyword_select_uncompat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKeyword_select_uncompat(ctx *Keyword_select_uncompatContext) {}

// EnterKeyword_alter_uncompat is called when production keyword_alter_uncompat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKeyword_alter_uncompat(ctx *Keyword_alter_uncompatContext) {}

// ExitKeyword_alter_uncompat is called when production keyword_alter_uncompat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKeyword_alter_uncompat(ctx *Keyword_alter_uncompatContext) {}

// EnterKeyword_in_uncompat is called when production keyword_in_uncompat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKeyword_in_uncompat(ctx *Keyword_in_uncompatContext) {}

// ExitKeyword_in_uncompat is called when production keyword_in_uncompat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKeyword_in_uncompat(ctx *Keyword_in_uncompatContext) {}

// EnterKeyword_window_uncompat is called when production keyword_window_uncompat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKeyword_window_uncompat(ctx *Keyword_window_uncompatContext) {}

// ExitKeyword_window_uncompat is called when production keyword_window_uncompat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKeyword_window_uncompat(ctx *Keyword_window_uncompatContext) {}

// EnterKeyword_hint_uncompat is called when production keyword_hint_uncompat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKeyword_hint_uncompat(ctx *Keyword_hint_uncompatContext) {}

// ExitKeyword_hint_uncompat is called when production keyword_hint_uncompat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKeyword_hint_uncompat(ctx *Keyword_hint_uncompatContext) {}

// EnterKeyword_as_compat is called when production keyword_as_compat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKeyword_as_compat(ctx *Keyword_as_compatContext) {}

// ExitKeyword_as_compat is called when production keyword_as_compat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKeyword_as_compat(ctx *Keyword_as_compatContext) {}

// EnterKeyword_compat is called when production keyword_compat is entered.
func (s *BaseYQLv1Antlr4Listener) EnterKeyword_compat(ctx *Keyword_compatContext) {}

// ExitKeyword_compat is called when production keyword_compat is exited.
func (s *BaseYQLv1Antlr4Listener) ExitKeyword_compat(ctx *Keyword_compatContext) {}

// EnterType_id is called when production type_id is entered.
func (s *BaseYQLv1Antlr4Listener) EnterType_id(ctx *Type_idContext) {}

// ExitType_id is called when production type_id is exited.
func (s *BaseYQLv1Antlr4Listener) ExitType_id(ctx *Type_idContext) {}

// EnterBool_value is called when production bool_value is entered.
func (s *BaseYQLv1Antlr4Listener) EnterBool_value(ctx *Bool_valueContext) {}

// ExitBool_value is called when production bool_value is exited.
func (s *BaseYQLv1Antlr4Listener) ExitBool_value(ctx *Bool_valueContext) {}

// EnterReal is called when production real is entered.
func (s *BaseYQLv1Antlr4Listener) EnterReal(ctx *RealContext) {}

// ExitReal is called when production real is exited.
func (s *BaseYQLv1Antlr4Listener) ExitReal(ctx *RealContext) {}

// EnterInteger is called when production integer is entered.
func (s *BaseYQLv1Antlr4Listener) EnterInteger(ctx *IntegerContext) {}

// ExitInteger is called when production integer is exited.
func (s *BaseYQLv1Antlr4Listener) ExitInteger(ctx *IntegerContext) {}
