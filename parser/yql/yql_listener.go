// Code generated from YQL.g4 by ANTLR 4.13.2. DO NOT EDIT.

package yql // YQL
import "github.com/antlr4-go/antlr/v4"


// YQLListener is a complete listener for a parse tree produced by YQLParser.
type YQLListener interface {
	antlr.ParseTreeListener

	// EnterSql_query is called when entering the sql_query production.
	EnterSql_query(c *Sql_queryContext)

	// EnterSql_stmt_list is called when entering the sql_stmt_list production.
	EnterSql_stmt_list(c *Sql_stmt_listContext)

	// EnterAnsi_sql_stmt_list is called when entering the ansi_sql_stmt_list production.
	EnterAnsi_sql_stmt_list(c *Ansi_sql_stmt_listContext)

	// EnterLambda_body is called when entering the lambda_body production.
	EnterLambda_body(c *Lambda_bodyContext)

	// EnterLambda_stmt is called when entering the lambda_stmt production.
	EnterLambda_stmt(c *Lambda_stmtContext)

	// EnterSql_stmt is called when entering the sql_stmt production.
	EnterSql_stmt(c *Sql_stmtContext)

	// EnterSql_stmt_core is called when entering the sql_stmt_core production.
	EnterSql_stmt_core(c *Sql_stmt_coreContext)

	// EnterExpr is called when entering the expr production.
	EnterExpr(c *ExprContext)

	// EnterOr_subexpr is called when entering the or_subexpr production.
	EnterOr_subexpr(c *Or_subexprContext)

	// EnterAnd_subexpr is called when entering the and_subexpr production.
	EnterAnd_subexpr(c *And_subexprContext)

	// EnterXor_subexpr is called when entering the xor_subexpr production.
	EnterXor_subexpr(c *Xor_subexprContext)

	// EnterDistinct_from_op is called when entering the distinct_from_op production.
	EnterDistinct_from_op(c *Distinct_from_opContext)

	// EnterCond_expr is called when entering the cond_expr production.
	EnterCond_expr(c *Cond_exprContext)

	// EnterMatch_op is called when entering the match_op production.
	EnterMatch_op(c *Match_opContext)

	// EnterEq_subexpr is called when entering the eq_subexpr production.
	EnterEq_subexpr(c *Eq_subexprContext)

	// EnterShift_right is called when entering the shift_right production.
	EnterShift_right(c *Shift_rightContext)

	// EnterRot_right is called when entering the rot_right production.
	EnterRot_right(c *Rot_rightContext)

	// EnterDouble_question is called when entering the double_question production.
	EnterDouble_question(c *Double_questionContext)

	// EnterNeq_subexpr is called when entering the neq_subexpr production.
	EnterNeq_subexpr(c *Neq_subexprContext)

	// EnterBit_subexpr is called when entering the bit_subexpr production.
	EnterBit_subexpr(c *Bit_subexprContext)

	// EnterAdd_subexpr is called when entering the add_subexpr production.
	EnterAdd_subexpr(c *Add_subexprContext)

	// EnterMul_subexpr is called when entering the mul_subexpr production.
	EnterMul_subexpr(c *Mul_subexprContext)

	// EnterCon_subexpr is called when entering the con_subexpr production.
	EnterCon_subexpr(c *Con_subexprContext)

	// EnterUnary_op is called when entering the unary_op production.
	EnterUnary_op(c *Unary_opContext)

	// EnterUnary_subexpr_suffix is called when entering the unary_subexpr_suffix production.
	EnterUnary_subexpr_suffix(c *Unary_subexpr_suffixContext)

	// EnterUnary_casual_subexpr is called when entering the unary_casual_subexpr production.
	EnterUnary_casual_subexpr(c *Unary_casual_subexprContext)

	// EnterIn_unary_casual_subexpr is called when entering the in_unary_casual_subexpr production.
	EnterIn_unary_casual_subexpr(c *In_unary_casual_subexprContext)

	// EnterUnary_subexpr is called when entering the unary_subexpr production.
	EnterUnary_subexpr(c *Unary_subexprContext)

	// EnterIn_unary_subexpr is called when entering the in_unary_subexpr production.
	EnterIn_unary_subexpr(c *In_unary_subexprContext)

	// EnterList_literal is called when entering the list_literal production.
	EnterList_literal(c *List_literalContext)

	// EnterExpr_dict_list is called when entering the expr_dict_list production.
	EnterExpr_dict_list(c *Expr_dict_listContext)

	// EnterDict_literal is called when entering the dict_literal production.
	EnterDict_literal(c *Dict_literalContext)

	// EnterExpr_struct_list is called when entering the expr_struct_list production.
	EnterExpr_struct_list(c *Expr_struct_listContext)

	// EnterStruct_literal is called when entering the struct_literal production.
	EnterStruct_literal(c *Struct_literalContext)

	// EnterAtom_expr is called when entering the atom_expr production.
	EnterAtom_expr(c *Atom_exprContext)

	// EnterIn_atom_expr is called when entering the in_atom_expr production.
	EnterIn_atom_expr(c *In_atom_exprContext)

	// EnterCast_expr is called when entering the cast_expr production.
	EnterCast_expr(c *Cast_exprContext)

	// EnterBitcast_expr is called when entering the bitcast_expr production.
	EnterBitcast_expr(c *Bitcast_exprContext)

	// EnterExists_expr is called when entering the exists_expr production.
	EnterExists_expr(c *Exists_exprContext)

	// EnterCase_expr is called when entering the case_expr production.
	EnterCase_expr(c *Case_exprContext)

	// EnterLambda is called when entering the lambda production.
	EnterLambda(c *LambdaContext)

	// EnterIn_expr is called when entering the in_expr production.
	EnterIn_expr(c *In_exprContext)

	// EnterJson_api_expr is called when entering the json_api_expr production.
	EnterJson_api_expr(c *Json_api_exprContext)

	// EnterJsonpath_spec is called when entering the jsonpath_spec production.
	EnterJsonpath_spec(c *Jsonpath_specContext)

	// EnterJson_variable_name is called when entering the json_variable_name production.
	EnterJson_variable_name(c *Json_variable_nameContext)

	// EnterJson_variable is called when entering the json_variable production.
	EnterJson_variable(c *Json_variableContext)

	// EnterJson_variables is called when entering the json_variables production.
	EnterJson_variables(c *Json_variablesContext)

	// EnterJson_common_args is called when entering the json_common_args production.
	EnterJson_common_args(c *Json_common_argsContext)

	// EnterJson_case_handler is called when entering the json_case_handler production.
	EnterJson_case_handler(c *Json_case_handlerContext)

	// EnterJson_value is called when entering the json_value production.
	EnterJson_value(c *Json_valueContext)

	// EnterJson_exists_handler is called when entering the json_exists_handler production.
	EnterJson_exists_handler(c *Json_exists_handlerContext)

	// EnterJson_exists is called when entering the json_exists production.
	EnterJson_exists(c *Json_existsContext)

	// EnterJson_query_wrapper is called when entering the json_query_wrapper production.
	EnterJson_query_wrapper(c *Json_query_wrapperContext)

	// EnterJson_query_handler is called when entering the json_query_handler production.
	EnterJson_query_handler(c *Json_query_handlerContext)

	// EnterJson_query is called when entering the json_query production.
	EnterJson_query(c *Json_queryContext)

	// EnterSmart_parenthesis is called when entering the smart_parenthesis production.
	EnterSmart_parenthesis(c *Smart_parenthesisContext)

	// EnterExpr_list is called when entering the expr_list production.
	EnterExpr_list(c *Expr_listContext)

	// EnterPure_column_list is called when entering the pure_column_list production.
	EnterPure_column_list(c *Pure_column_listContext)

	// EnterPure_column_or_named is called when entering the pure_column_or_named production.
	EnterPure_column_or_named(c *Pure_column_or_namedContext)

	// EnterPure_column_or_named_list is called when entering the pure_column_or_named_list production.
	EnterPure_column_or_named_list(c *Pure_column_or_named_listContext)

	// EnterColumn_name is called when entering the column_name production.
	EnterColumn_name(c *Column_nameContext)

	// EnterWithout_column_name is called when entering the without_column_name production.
	EnterWithout_column_name(c *Without_column_nameContext)

	// EnterColumn_list is called when entering the column_list production.
	EnterColumn_list(c *Column_listContext)

	// EnterWithout_column_list is called when entering the without_column_list production.
	EnterWithout_column_list(c *Without_column_listContext)

	// EnterNamed_expr is called when entering the named_expr production.
	EnterNamed_expr(c *Named_exprContext)

	// EnterNamed_expr_list is called when entering the named_expr_list production.
	EnterNamed_expr_list(c *Named_expr_listContext)

	// EnterInvoke_expr is called when entering the invoke_expr production.
	EnterInvoke_expr(c *Invoke_exprContext)

	// EnterInvoke_expr_tail is called when entering the invoke_expr_tail production.
	EnterInvoke_expr_tail(c *Invoke_expr_tailContext)

	// EnterUsing_call_expr is called when entering the using_call_expr production.
	EnterUsing_call_expr(c *Using_call_exprContext)

	// EnterKey_expr is called when entering the key_expr production.
	EnterKey_expr(c *Key_exprContext)

	// EnterWhen_expr is called when entering the when_expr production.
	EnterWhen_expr(c *When_exprContext)

	// EnterLiteral_value is called when entering the literal_value production.
	EnterLiteral_value(c *Literal_valueContext)

	// EnterBind_parameter is called when entering the bind_parameter production.
	EnterBind_parameter(c *Bind_parameterContext)

	// EnterOpt_bind_parameter is called when entering the opt_bind_parameter production.
	EnterOpt_bind_parameter(c *Opt_bind_parameterContext)

	// EnterBind_parameter_list is called when entering the bind_parameter_list production.
	EnterBind_parameter_list(c *Bind_parameter_listContext)

	// EnterNamed_bind_parameter is called when entering the named_bind_parameter production.
	EnterNamed_bind_parameter(c *Named_bind_parameterContext)

	// EnterNamed_bind_parameter_list is called when entering the named_bind_parameter_list production.
	EnterNamed_bind_parameter_list(c *Named_bind_parameter_listContext)

	// EnterSigned_number is called when entering the signed_number production.
	EnterSigned_number(c *Signed_numberContext)

	// EnterType_name_simple is called when entering the type_name_simple production.
	EnterType_name_simple(c *Type_name_simpleContext)

	// EnterInteger_or_bind is called when entering the integer_or_bind production.
	EnterInteger_or_bind(c *Integer_or_bindContext)

	// EnterType_name_tag is called when entering the type_name_tag production.
	EnterType_name_tag(c *Type_name_tagContext)

	// EnterStruct_arg is called when entering the struct_arg production.
	EnterStruct_arg(c *Struct_argContext)

	// EnterStruct_arg_positional is called when entering the struct_arg_positional production.
	EnterStruct_arg_positional(c *Struct_arg_positionalContext)

	// EnterVariant_arg is called when entering the variant_arg production.
	EnterVariant_arg(c *Variant_argContext)

	// EnterCallable_arg is called when entering the callable_arg production.
	EnterCallable_arg(c *Callable_argContext)

	// EnterCallable_arg_list is called when entering the callable_arg_list production.
	EnterCallable_arg_list(c *Callable_arg_listContext)

	// EnterType_name_decimal is called when entering the type_name_decimal production.
	EnterType_name_decimal(c *Type_name_decimalContext)

	// EnterType_name_optional is called when entering the type_name_optional production.
	EnterType_name_optional(c *Type_name_optionalContext)

	// EnterType_name_tuple is called when entering the type_name_tuple production.
	EnterType_name_tuple(c *Type_name_tupleContext)

	// EnterType_name_struct is called when entering the type_name_struct production.
	EnterType_name_struct(c *Type_name_structContext)

	// EnterType_name_variant is called when entering the type_name_variant production.
	EnterType_name_variant(c *Type_name_variantContext)

	// EnterType_name_list is called when entering the type_name_list production.
	EnterType_name_list(c *Type_name_listContext)

	// EnterType_name_stream is called when entering the type_name_stream production.
	EnterType_name_stream(c *Type_name_streamContext)

	// EnterType_name_flow is called when entering the type_name_flow production.
	EnterType_name_flow(c *Type_name_flowContext)

	// EnterType_name_dict is called when entering the type_name_dict production.
	EnterType_name_dict(c *Type_name_dictContext)

	// EnterType_name_set is called when entering the type_name_set production.
	EnterType_name_set(c *Type_name_setContext)

	// EnterType_name_enum is called when entering the type_name_enum production.
	EnterType_name_enum(c *Type_name_enumContext)

	// EnterType_name_resource is called when entering the type_name_resource production.
	EnterType_name_resource(c *Type_name_resourceContext)

	// EnterType_name_tagged is called when entering the type_name_tagged production.
	EnterType_name_tagged(c *Type_name_taggedContext)

	// EnterType_name_callable is called when entering the type_name_callable production.
	EnterType_name_callable(c *Type_name_callableContext)

	// EnterType_name_composite is called when entering the type_name_composite production.
	EnterType_name_composite(c *Type_name_compositeContext)

	// EnterType_name is called when entering the type_name production.
	EnterType_name(c *Type_nameContext)

	// EnterType_name_or_bind is called when entering the type_name_or_bind production.
	EnterType_name_or_bind(c *Type_name_or_bindContext)

	// EnterValue_constructor_literal is called when entering the value_constructor_literal production.
	EnterValue_constructor_literal(c *Value_constructor_literalContext)

	// EnterValue_constructor is called when entering the value_constructor production.
	EnterValue_constructor(c *Value_constructorContext)

	// EnterDeclare_stmt is called when entering the declare_stmt production.
	EnterDeclare_stmt(c *Declare_stmtContext)

	// EnterModule_path is called when entering the module_path production.
	EnterModule_path(c *Module_pathContext)

	// EnterImport_stmt is called when entering the import_stmt production.
	EnterImport_stmt(c *Import_stmtContext)

	// EnterExport_stmt is called when entering the export_stmt production.
	EnterExport_stmt(c *Export_stmtContext)

	// EnterCall_action is called when entering the call_action production.
	EnterCall_action(c *Call_actionContext)

	// EnterInline_action is called when entering the inline_action production.
	EnterInline_action(c *Inline_actionContext)

	// EnterDo_stmt is called when entering the do_stmt production.
	EnterDo_stmt(c *Do_stmtContext)

	// EnterPragma_stmt is called when entering the pragma_stmt production.
	EnterPragma_stmt(c *Pragma_stmtContext)

	// EnterPragma_value is called when entering the pragma_value production.
	EnterPragma_value(c *Pragma_valueContext)

	// EnterSort_specification is called when entering the sort_specification production.
	EnterSort_specification(c *Sort_specificationContext)

	// EnterSort_specification_list is called when entering the sort_specification_list production.
	EnterSort_specification_list(c *Sort_specification_listContext)

	// EnterSelect_stmt is called when entering the select_stmt production.
	EnterSelect_stmt(c *Select_stmtContext)

	// EnterSelect_unparenthesized_stmt is called when entering the select_unparenthesized_stmt production.
	EnterSelect_unparenthesized_stmt(c *Select_unparenthesized_stmtContext)

	// EnterSelect_kind_parenthesis is called when entering the select_kind_parenthesis production.
	EnterSelect_kind_parenthesis(c *Select_kind_parenthesisContext)

	// EnterSelect_op is called when entering the select_op production.
	EnterSelect_op(c *Select_opContext)

	// EnterSelect_kind_partial is called when entering the select_kind_partial production.
	EnterSelect_kind_partial(c *Select_kind_partialContext)

	// EnterSelect_kind is called when entering the select_kind production.
	EnterSelect_kind(c *Select_kindContext)

	// EnterProcess_core is called when entering the process_core production.
	EnterProcess_core(c *Process_coreContext)

	// EnterExternal_call_param is called when entering the external_call_param production.
	EnterExternal_call_param(c *External_call_paramContext)

	// EnterExternal_call_settings is called when entering the external_call_settings production.
	EnterExternal_call_settings(c *External_call_settingsContext)

	// EnterReduce_core is called when entering the reduce_core production.
	EnterReduce_core(c *Reduce_coreContext)

	// EnterOpt_set_quantifier is called when entering the opt_set_quantifier production.
	EnterOpt_set_quantifier(c *Opt_set_quantifierContext)

	// EnterSelect_core is called when entering the select_core production.
	EnterSelect_core(c *Select_coreContext)

	// EnterRow_pattern_recognition_clause is called when entering the row_pattern_recognition_clause production.
	EnterRow_pattern_recognition_clause(c *Row_pattern_recognition_clauseContext)

	// EnterRow_pattern_rows_per_match is called when entering the row_pattern_rows_per_match production.
	EnterRow_pattern_rows_per_match(c *Row_pattern_rows_per_matchContext)

	// EnterRow_pattern_empty_match_handling is called when entering the row_pattern_empty_match_handling production.
	EnterRow_pattern_empty_match_handling(c *Row_pattern_empty_match_handlingContext)

	// EnterRow_pattern_measures is called when entering the row_pattern_measures production.
	EnterRow_pattern_measures(c *Row_pattern_measuresContext)

	// EnterRow_pattern_measure_list is called when entering the row_pattern_measure_list production.
	EnterRow_pattern_measure_list(c *Row_pattern_measure_listContext)

	// EnterRow_pattern_measure_definition is called when entering the row_pattern_measure_definition production.
	EnterRow_pattern_measure_definition(c *Row_pattern_measure_definitionContext)

	// EnterRow_pattern_common_syntax is called when entering the row_pattern_common_syntax production.
	EnterRow_pattern_common_syntax(c *Row_pattern_common_syntaxContext)

	// EnterRow_pattern_skip_to is called when entering the row_pattern_skip_to production.
	EnterRow_pattern_skip_to(c *Row_pattern_skip_toContext)

	// EnterRow_pattern_skip_to_variable_name is called when entering the row_pattern_skip_to_variable_name production.
	EnterRow_pattern_skip_to_variable_name(c *Row_pattern_skip_to_variable_nameContext)

	// EnterRow_pattern_initial_or_seek is called when entering the row_pattern_initial_or_seek production.
	EnterRow_pattern_initial_or_seek(c *Row_pattern_initial_or_seekContext)

	// EnterRow_pattern is called when entering the row_pattern production.
	EnterRow_pattern(c *Row_patternContext)

	// EnterRow_pattern_term is called when entering the row_pattern_term production.
	EnterRow_pattern_term(c *Row_pattern_termContext)

	// EnterRow_pattern_factor is called when entering the row_pattern_factor production.
	EnterRow_pattern_factor(c *Row_pattern_factorContext)

	// EnterRow_pattern_quantifier is called when entering the row_pattern_quantifier production.
	EnterRow_pattern_quantifier(c *Row_pattern_quantifierContext)

	// EnterRow_pattern_primary is called when entering the row_pattern_primary production.
	EnterRow_pattern_primary(c *Row_pattern_primaryContext)

	// EnterRow_pattern_primary_variable_name is called when entering the row_pattern_primary_variable_name production.
	EnterRow_pattern_primary_variable_name(c *Row_pattern_primary_variable_nameContext)

	// EnterRow_pattern_permute is called when entering the row_pattern_permute production.
	EnterRow_pattern_permute(c *Row_pattern_permuteContext)

	// EnterRow_pattern_subset_clause is called when entering the row_pattern_subset_clause production.
	EnterRow_pattern_subset_clause(c *Row_pattern_subset_clauseContext)

	// EnterRow_pattern_subset_list is called when entering the row_pattern_subset_list production.
	EnterRow_pattern_subset_list(c *Row_pattern_subset_listContext)

	// EnterRow_pattern_subset_item is called when entering the row_pattern_subset_item production.
	EnterRow_pattern_subset_item(c *Row_pattern_subset_itemContext)

	// EnterRow_pattern_subset_item_variable_name is called when entering the row_pattern_subset_item_variable_name production.
	EnterRow_pattern_subset_item_variable_name(c *Row_pattern_subset_item_variable_nameContext)

	// EnterRow_pattern_subset_rhs is called when entering the row_pattern_subset_rhs production.
	EnterRow_pattern_subset_rhs(c *Row_pattern_subset_rhsContext)

	// EnterRow_pattern_subset_rhs_variable_name is called when entering the row_pattern_subset_rhs_variable_name production.
	EnterRow_pattern_subset_rhs_variable_name(c *Row_pattern_subset_rhs_variable_nameContext)

	// EnterRow_pattern_definition_list is called when entering the row_pattern_definition_list production.
	EnterRow_pattern_definition_list(c *Row_pattern_definition_listContext)

	// EnterRow_pattern_definition is called when entering the row_pattern_definition production.
	EnterRow_pattern_definition(c *Row_pattern_definitionContext)

	// EnterRow_pattern_definition_variable_name is called when entering the row_pattern_definition_variable_name production.
	EnterRow_pattern_definition_variable_name(c *Row_pattern_definition_variable_nameContext)

	// EnterRow_pattern_definition_search_condition is called when entering the row_pattern_definition_search_condition production.
	EnterRow_pattern_definition_search_condition(c *Row_pattern_definition_search_conditionContext)

	// EnterSearch_condition is called when entering the search_condition production.
	EnterSearch_condition(c *Search_conditionContext)

	// EnterRow_pattern_variable_name is called when entering the row_pattern_variable_name production.
	EnterRow_pattern_variable_name(c *Row_pattern_variable_nameContext)

	// EnterOrder_by_clause is called when entering the order_by_clause production.
	EnterOrder_by_clause(c *Order_by_clauseContext)

	// EnterExt_order_by_clause is called when entering the ext_order_by_clause production.
	EnterExt_order_by_clause(c *Ext_order_by_clauseContext)

	// EnterGroup_by_clause is called when entering the group_by_clause production.
	EnterGroup_by_clause(c *Group_by_clauseContext)

	// EnterGrouping_element_list is called when entering the grouping_element_list production.
	EnterGrouping_element_list(c *Grouping_element_listContext)

	// EnterGrouping_element is called when entering the grouping_element production.
	EnterGrouping_element(c *Grouping_elementContext)

	// EnterOrdinary_grouping_set is called when entering the ordinary_grouping_set production.
	EnterOrdinary_grouping_set(c *Ordinary_grouping_setContext)

	// EnterOrdinary_grouping_set_list is called when entering the ordinary_grouping_set_list production.
	EnterOrdinary_grouping_set_list(c *Ordinary_grouping_set_listContext)

	// EnterRollup_list is called when entering the rollup_list production.
	EnterRollup_list(c *Rollup_listContext)

	// EnterCube_list is called when entering the cube_list production.
	EnterCube_list(c *Cube_listContext)

	// EnterGrouping_sets_specification is called when entering the grouping_sets_specification production.
	EnterGrouping_sets_specification(c *Grouping_sets_specificationContext)

	// EnterHopping_window_specification is called when entering the hopping_window_specification production.
	EnterHopping_window_specification(c *Hopping_window_specificationContext)

	// EnterResult_column is called when entering the result_column production.
	EnterResult_column(c *Result_columnContext)

	// EnterJoin_source is called when entering the join_source production.
	EnterJoin_source(c *Join_sourceContext)

	// EnterNamed_column is called when entering the named_column production.
	EnterNamed_column(c *Named_columnContext)

	// EnterFlatten_by_arg is called when entering the flatten_by_arg production.
	EnterFlatten_by_arg(c *Flatten_by_argContext)

	// EnterFlatten_source is called when entering the flatten_source production.
	EnterFlatten_source(c *Flatten_sourceContext)

	// EnterNamed_single_source is called when entering the named_single_source production.
	EnterNamed_single_source(c *Named_single_sourceContext)

	// EnterSingle_source is called when entering the single_source production.
	EnterSingle_source(c *Single_sourceContext)

	// EnterSample_clause is called when entering the sample_clause production.
	EnterSample_clause(c *Sample_clauseContext)

	// EnterTablesample_clause is called when entering the tablesample_clause production.
	EnterTablesample_clause(c *Tablesample_clauseContext)

	// EnterSampling_mode is called when entering the sampling_mode production.
	EnterSampling_mode(c *Sampling_modeContext)

	// EnterRepeatable_clause is called when entering the repeatable_clause production.
	EnterRepeatable_clause(c *Repeatable_clauseContext)

	// EnterJoin_op is called when entering the join_op production.
	EnterJoin_op(c *Join_opContext)

	// EnterJoin_constraint is called when entering the join_constraint production.
	EnterJoin_constraint(c *Join_constraintContext)

	// EnterReturning_columns_list is called when entering the returning_columns_list production.
	EnterReturning_columns_list(c *Returning_columns_listContext)

	// EnterInto_table_stmt is called when entering the into_table_stmt production.
	EnterInto_table_stmt(c *Into_table_stmtContext)

	// EnterInto_values_source is called when entering the into_values_source production.
	EnterInto_values_source(c *Into_values_sourceContext)

	// EnterValues_stmt is called when entering the values_stmt production.
	EnterValues_stmt(c *Values_stmtContext)

	// EnterValues_source is called when entering the values_source production.
	EnterValues_source(c *Values_sourceContext)

	// EnterValues_source_row_list is called when entering the values_source_row_list production.
	EnterValues_source_row_list(c *Values_source_row_listContext)

	// EnterValues_source_row is called when entering the values_source_row production.
	EnterValues_source_row(c *Values_source_rowContext)

	// EnterSimple_values_source is called when entering the simple_values_source production.
	EnterSimple_values_source(c *Simple_values_sourceContext)

	// EnterCreate_external_data_source_stmt is called when entering the create_external_data_source_stmt production.
	EnterCreate_external_data_source_stmt(c *Create_external_data_source_stmtContext)

	// EnterAlter_external_data_source_stmt is called when entering the alter_external_data_source_stmt production.
	EnterAlter_external_data_source_stmt(c *Alter_external_data_source_stmtContext)

	// EnterAlter_external_data_source_action is called when entering the alter_external_data_source_action production.
	EnterAlter_external_data_source_action(c *Alter_external_data_source_actionContext)

	// EnterDrop_external_data_source_stmt is called when entering the drop_external_data_source_stmt production.
	EnterDrop_external_data_source_stmt(c *Drop_external_data_source_stmtContext)

	// EnterCreate_view_stmt is called when entering the create_view_stmt production.
	EnterCreate_view_stmt(c *Create_view_stmtContext)

	// EnterDrop_view_stmt is called when entering the drop_view_stmt production.
	EnterDrop_view_stmt(c *Drop_view_stmtContext)

	// EnterUpsert_object_stmt is called when entering the upsert_object_stmt production.
	EnterUpsert_object_stmt(c *Upsert_object_stmtContext)

	// EnterCreate_object_stmt is called when entering the create_object_stmt production.
	EnterCreate_object_stmt(c *Create_object_stmtContext)

	// EnterCreate_object_features is called when entering the create_object_features production.
	EnterCreate_object_features(c *Create_object_featuresContext)

	// EnterAlter_object_stmt is called when entering the alter_object_stmt production.
	EnterAlter_object_stmt(c *Alter_object_stmtContext)

	// EnterAlter_object_features is called when entering the alter_object_features production.
	EnterAlter_object_features(c *Alter_object_featuresContext)

	// EnterDrop_object_stmt is called when entering the drop_object_stmt production.
	EnterDrop_object_stmt(c *Drop_object_stmtContext)

	// EnterDrop_object_features is called when entering the drop_object_features production.
	EnterDrop_object_features(c *Drop_object_featuresContext)

	// EnterObject_feature_value is called when entering the object_feature_value production.
	EnterObject_feature_value(c *Object_feature_valueContext)

	// EnterObject_feature_kv is called when entering the object_feature_kv production.
	EnterObject_feature_kv(c *Object_feature_kvContext)

	// EnterObject_feature_flag is called when entering the object_feature_flag production.
	EnterObject_feature_flag(c *Object_feature_flagContext)

	// EnterObject_feature is called when entering the object_feature production.
	EnterObject_feature(c *Object_featureContext)

	// EnterObject_features is called when entering the object_features production.
	EnterObject_features(c *Object_featuresContext)

	// EnterObject_type_ref is called when entering the object_type_ref production.
	EnterObject_type_ref(c *Object_type_refContext)

	// EnterCreate_table_stmt is called when entering the create_table_stmt production.
	EnterCreate_table_stmt(c *Create_table_stmtContext)

	// EnterCreate_table_entry is called when entering the create_table_entry production.
	EnterCreate_table_entry(c *Create_table_entryContext)

	// EnterCreate_backup_collection_stmt is called when entering the create_backup_collection_stmt production.
	EnterCreate_backup_collection_stmt(c *Create_backup_collection_stmtContext)

	// EnterAlter_backup_collection_stmt is called when entering the alter_backup_collection_stmt production.
	EnterAlter_backup_collection_stmt(c *Alter_backup_collection_stmtContext)

	// EnterDrop_backup_collection_stmt is called when entering the drop_backup_collection_stmt production.
	EnterDrop_backup_collection_stmt(c *Drop_backup_collection_stmtContext)

	// EnterCreate_backup_collection_entries is called when entering the create_backup_collection_entries production.
	EnterCreate_backup_collection_entries(c *Create_backup_collection_entriesContext)

	// EnterCreate_backup_collection_entries_many is called when entering the create_backup_collection_entries_many production.
	EnterCreate_backup_collection_entries_many(c *Create_backup_collection_entries_manyContext)

	// EnterTable_list is called when entering the table_list production.
	EnterTable_list(c *Table_listContext)

	// EnterAlter_backup_collection_actions is called when entering the alter_backup_collection_actions production.
	EnterAlter_backup_collection_actions(c *Alter_backup_collection_actionsContext)

	// EnterAlter_backup_collection_action is called when entering the alter_backup_collection_action production.
	EnterAlter_backup_collection_action(c *Alter_backup_collection_actionContext)

	// EnterAlter_backup_collection_entries is called when entering the alter_backup_collection_entries production.
	EnterAlter_backup_collection_entries(c *Alter_backup_collection_entriesContext)

	// EnterAlter_backup_collection_entry is called when entering the alter_backup_collection_entry production.
	EnterAlter_backup_collection_entry(c *Alter_backup_collection_entryContext)

	// EnterBackup_collection is called when entering the backup_collection production.
	EnterBackup_collection(c *Backup_collectionContext)

	// EnterBackup_collection_settings is called when entering the backup_collection_settings production.
	EnterBackup_collection_settings(c *Backup_collection_settingsContext)

	// EnterBackup_collection_settings_entry is called when entering the backup_collection_settings_entry production.
	EnterBackup_collection_settings_entry(c *Backup_collection_settings_entryContext)

	// EnterBackup_stmt is called when entering the backup_stmt production.
	EnterBackup_stmt(c *Backup_stmtContext)

	// EnterRestore_stmt is called when entering the restore_stmt production.
	EnterRestore_stmt(c *Restore_stmtContext)

	// EnterTable_inherits is called when entering the table_inherits production.
	EnterTable_inherits(c *Table_inheritsContext)

	// EnterTable_partition_by is called when entering the table_partition_by production.
	EnterTable_partition_by(c *Table_partition_byContext)

	// EnterWith_table_settings is called when entering the with_table_settings production.
	EnterWith_table_settings(c *With_table_settingsContext)

	// EnterTable_tablestore is called when entering the table_tablestore production.
	EnterTable_tablestore(c *Table_tablestoreContext)

	// EnterTable_settings_entry is called when entering the table_settings_entry production.
	EnterTable_settings_entry(c *Table_settings_entryContext)

	// EnterTable_as_source is called when entering the table_as_source production.
	EnterTable_as_source(c *Table_as_sourceContext)

	// EnterAlter_table_stmt is called when entering the alter_table_stmt production.
	EnterAlter_table_stmt(c *Alter_table_stmtContext)

	// EnterAlter_table_action is called when entering the alter_table_action production.
	EnterAlter_table_action(c *Alter_table_actionContext)

	// EnterAlter_external_table_stmt is called when entering the alter_external_table_stmt production.
	EnterAlter_external_table_stmt(c *Alter_external_table_stmtContext)

	// EnterAlter_external_table_action is called when entering the alter_external_table_action production.
	EnterAlter_external_table_action(c *Alter_external_table_actionContext)

	// EnterAlter_table_store_stmt is called when entering the alter_table_store_stmt production.
	EnterAlter_table_store_stmt(c *Alter_table_store_stmtContext)

	// EnterAlter_table_store_action is called when entering the alter_table_store_action production.
	EnterAlter_table_store_action(c *Alter_table_store_actionContext)

	// EnterAlter_table_add_column is called when entering the alter_table_add_column production.
	EnterAlter_table_add_column(c *Alter_table_add_columnContext)

	// EnterAlter_table_drop_column is called when entering the alter_table_drop_column production.
	EnterAlter_table_drop_column(c *Alter_table_drop_columnContext)

	// EnterAlter_table_alter_column is called when entering the alter_table_alter_column production.
	EnterAlter_table_alter_column(c *Alter_table_alter_columnContext)

	// EnterAlter_table_alter_column_drop_not_null is called when entering the alter_table_alter_column_drop_not_null production.
	EnterAlter_table_alter_column_drop_not_null(c *Alter_table_alter_column_drop_not_nullContext)

	// EnterAlter_table_add_column_family is called when entering the alter_table_add_column_family production.
	EnterAlter_table_add_column_family(c *Alter_table_add_column_familyContext)

	// EnterAlter_table_alter_column_family is called when entering the alter_table_alter_column_family production.
	EnterAlter_table_alter_column_family(c *Alter_table_alter_column_familyContext)

	// EnterAlter_table_set_table_setting_uncompat is called when entering the alter_table_set_table_setting_uncompat production.
	EnterAlter_table_set_table_setting_uncompat(c *Alter_table_set_table_setting_uncompatContext)

	// EnterAlter_table_set_table_setting_compat is called when entering the alter_table_set_table_setting_compat production.
	EnterAlter_table_set_table_setting_compat(c *Alter_table_set_table_setting_compatContext)

	// EnterAlter_table_reset_table_setting is called when entering the alter_table_reset_table_setting production.
	EnterAlter_table_reset_table_setting(c *Alter_table_reset_table_settingContext)

	// EnterAlter_table_add_index is called when entering the alter_table_add_index production.
	EnterAlter_table_add_index(c *Alter_table_add_indexContext)

	// EnterAlter_table_drop_index is called when entering the alter_table_drop_index production.
	EnterAlter_table_drop_index(c *Alter_table_drop_indexContext)

	// EnterAlter_table_rename_to is called when entering the alter_table_rename_to production.
	EnterAlter_table_rename_to(c *Alter_table_rename_toContext)

	// EnterAlter_table_rename_index_to is called when entering the alter_table_rename_index_to production.
	EnterAlter_table_rename_index_to(c *Alter_table_rename_index_toContext)

	// EnterAlter_table_add_changefeed is called when entering the alter_table_add_changefeed production.
	EnterAlter_table_add_changefeed(c *Alter_table_add_changefeedContext)

	// EnterAlter_table_alter_changefeed is called when entering the alter_table_alter_changefeed production.
	EnterAlter_table_alter_changefeed(c *Alter_table_alter_changefeedContext)

	// EnterAlter_table_drop_changefeed is called when entering the alter_table_drop_changefeed production.
	EnterAlter_table_drop_changefeed(c *Alter_table_drop_changefeedContext)

	// EnterAlter_table_alter_index is called when entering the alter_table_alter_index production.
	EnterAlter_table_alter_index(c *Alter_table_alter_indexContext)

	// EnterColumn_schema is called when entering the column_schema production.
	EnterColumn_schema(c *Column_schemaContext)

	// EnterFamily_relation is called when entering the family_relation production.
	EnterFamily_relation(c *Family_relationContext)

	// EnterOpt_column_constraints is called when entering the opt_column_constraints production.
	EnterOpt_column_constraints(c *Opt_column_constraintsContext)

	// EnterColumn_order_by_specification is called when entering the column_order_by_specification production.
	EnterColumn_order_by_specification(c *Column_order_by_specificationContext)

	// EnterTable_constraint is called when entering the table_constraint production.
	EnterTable_constraint(c *Table_constraintContext)

	// EnterTable_index is called when entering the table_index production.
	EnterTable_index(c *Table_indexContext)

	// EnterTable_index_type is called when entering the table_index_type production.
	EnterTable_index_type(c *Table_index_typeContext)

	// EnterGlobal_index is called when entering the global_index production.
	EnterGlobal_index(c *Global_indexContext)

	// EnterLocal_index is called when entering the local_index production.
	EnterLocal_index(c *Local_indexContext)

	// EnterIndex_subtype is called when entering the index_subtype production.
	EnterIndex_subtype(c *Index_subtypeContext)

	// EnterWith_index_settings is called when entering the with_index_settings production.
	EnterWith_index_settings(c *With_index_settingsContext)

	// EnterIndex_setting_entry is called when entering the index_setting_entry production.
	EnterIndex_setting_entry(c *Index_setting_entryContext)

	// EnterIndex_setting_value is called when entering the index_setting_value production.
	EnterIndex_setting_value(c *Index_setting_valueContext)

	// EnterChangefeed is called when entering the changefeed production.
	EnterChangefeed(c *ChangefeedContext)

	// EnterChangefeed_settings is called when entering the changefeed_settings production.
	EnterChangefeed_settings(c *Changefeed_settingsContext)

	// EnterChangefeed_settings_entry is called when entering the changefeed_settings_entry production.
	EnterChangefeed_settings_entry(c *Changefeed_settings_entryContext)

	// EnterChangefeed_setting_value is called when entering the changefeed_setting_value production.
	EnterChangefeed_setting_value(c *Changefeed_setting_valueContext)

	// EnterChangefeed_alter_settings is called when entering the changefeed_alter_settings production.
	EnterChangefeed_alter_settings(c *Changefeed_alter_settingsContext)

	// EnterAlter_table_setting_entry is called when entering the alter_table_setting_entry production.
	EnterAlter_table_setting_entry(c *Alter_table_setting_entryContext)

	// EnterTable_setting_value is called when entering the table_setting_value production.
	EnterTable_setting_value(c *Table_setting_valueContext)

	// EnterTtl_tier_list is called when entering the ttl_tier_list production.
	EnterTtl_tier_list(c *Ttl_tier_listContext)

	// EnterTtl_tier_action is called when entering the ttl_tier_action production.
	EnterTtl_tier_action(c *Ttl_tier_actionContext)

	// EnterFamily_entry is called when entering the family_entry production.
	EnterFamily_entry(c *Family_entryContext)

	// EnterFamily_settings is called when entering the family_settings production.
	EnterFamily_settings(c *Family_settingsContext)

	// EnterFamily_settings_entry is called when entering the family_settings_entry production.
	EnterFamily_settings_entry(c *Family_settings_entryContext)

	// EnterFamily_setting_value is called when entering the family_setting_value production.
	EnterFamily_setting_value(c *Family_setting_valueContext)

	// EnterSplit_boundaries is called when entering the split_boundaries production.
	EnterSplit_boundaries(c *Split_boundariesContext)

	// EnterLiteral_value_list is called when entering the literal_value_list production.
	EnterLiteral_value_list(c *Literal_value_listContext)

	// EnterAlter_table_alter_index_action is called when entering the alter_table_alter_index_action production.
	EnterAlter_table_alter_index_action(c *Alter_table_alter_index_actionContext)

	// EnterDrop_table_stmt is called when entering the drop_table_stmt production.
	EnterDrop_table_stmt(c *Drop_table_stmtContext)

	// EnterCreate_user_stmt is called when entering the create_user_stmt production.
	EnterCreate_user_stmt(c *Create_user_stmtContext)

	// EnterAlter_user_stmt is called when entering the alter_user_stmt production.
	EnterAlter_user_stmt(c *Alter_user_stmtContext)

	// EnterCreate_group_stmt is called when entering the create_group_stmt production.
	EnterCreate_group_stmt(c *Create_group_stmtContext)

	// EnterAlter_group_stmt is called when entering the alter_group_stmt production.
	EnterAlter_group_stmt(c *Alter_group_stmtContext)

	// EnterDrop_role_stmt is called when entering the drop_role_stmt production.
	EnterDrop_role_stmt(c *Drop_role_stmtContext)

	// EnterRole_name is called when entering the role_name production.
	EnterRole_name(c *Role_nameContext)

	// EnterCreate_user_option is called when entering the create_user_option production.
	EnterCreate_user_option(c *Create_user_optionContext)

	// EnterPassword_option is called when entering the password_option production.
	EnterPassword_option(c *Password_optionContext)

	// EnterLogin_option is called when entering the login_option production.
	EnterLogin_option(c *Login_optionContext)

	// EnterGrant_permissions_stmt is called when entering the grant_permissions_stmt production.
	EnterGrant_permissions_stmt(c *Grant_permissions_stmtContext)

	// EnterRevoke_permissions_stmt is called when entering the revoke_permissions_stmt production.
	EnterRevoke_permissions_stmt(c *Revoke_permissions_stmtContext)

	// EnterPermission_id is called when entering the permission_id production.
	EnterPermission_id(c *Permission_idContext)

	// EnterPermission_name is called when entering the permission_name production.
	EnterPermission_name(c *Permission_nameContext)

	// EnterPermission_name_target is called when entering the permission_name_target production.
	EnterPermission_name_target(c *Permission_name_targetContext)

	// EnterCreate_resource_pool_stmt is called when entering the create_resource_pool_stmt production.
	EnterCreate_resource_pool_stmt(c *Create_resource_pool_stmtContext)

	// EnterAlter_resource_pool_stmt is called when entering the alter_resource_pool_stmt production.
	EnterAlter_resource_pool_stmt(c *Alter_resource_pool_stmtContext)

	// EnterAlter_resource_pool_action is called when entering the alter_resource_pool_action production.
	EnterAlter_resource_pool_action(c *Alter_resource_pool_actionContext)

	// EnterDrop_resource_pool_stmt is called when entering the drop_resource_pool_stmt production.
	EnterDrop_resource_pool_stmt(c *Drop_resource_pool_stmtContext)

	// EnterCreate_resource_pool_classifier_stmt is called when entering the create_resource_pool_classifier_stmt production.
	EnterCreate_resource_pool_classifier_stmt(c *Create_resource_pool_classifier_stmtContext)

	// EnterAlter_resource_pool_classifier_stmt is called when entering the alter_resource_pool_classifier_stmt production.
	EnterAlter_resource_pool_classifier_stmt(c *Alter_resource_pool_classifier_stmtContext)

	// EnterAlter_resource_pool_classifier_action is called when entering the alter_resource_pool_classifier_action production.
	EnterAlter_resource_pool_classifier_action(c *Alter_resource_pool_classifier_actionContext)

	// EnterDrop_resource_pool_classifier_stmt is called when entering the drop_resource_pool_classifier_stmt production.
	EnterDrop_resource_pool_classifier_stmt(c *Drop_resource_pool_classifier_stmtContext)

	// EnterCreate_replication_stmt is called when entering the create_replication_stmt production.
	EnterCreate_replication_stmt(c *Create_replication_stmtContext)

	// EnterReplication_target is called when entering the replication_target production.
	EnterReplication_target(c *Replication_targetContext)

	// EnterReplication_settings is called when entering the replication_settings production.
	EnterReplication_settings(c *Replication_settingsContext)

	// EnterReplication_settings_entry is called when entering the replication_settings_entry production.
	EnterReplication_settings_entry(c *Replication_settings_entryContext)

	// EnterAlter_replication_stmt is called when entering the alter_replication_stmt production.
	EnterAlter_replication_stmt(c *Alter_replication_stmtContext)

	// EnterAlter_replication_action is called when entering the alter_replication_action production.
	EnterAlter_replication_action(c *Alter_replication_actionContext)

	// EnterAlter_replication_set_setting is called when entering the alter_replication_set_setting production.
	EnterAlter_replication_set_setting(c *Alter_replication_set_settingContext)

	// EnterDrop_replication_stmt is called when entering the drop_replication_stmt production.
	EnterDrop_replication_stmt(c *Drop_replication_stmtContext)

	// EnterAction_or_subquery_args is called when entering the action_or_subquery_args production.
	EnterAction_or_subquery_args(c *Action_or_subquery_argsContext)

	// EnterDefine_action_or_subquery_stmt is called when entering the define_action_or_subquery_stmt production.
	EnterDefine_action_or_subquery_stmt(c *Define_action_or_subquery_stmtContext)

	// EnterDefine_action_or_subquery_body is called when entering the define_action_or_subquery_body production.
	EnterDefine_action_or_subquery_body(c *Define_action_or_subquery_bodyContext)

	// EnterIf_stmt is called when entering the if_stmt production.
	EnterIf_stmt(c *If_stmtContext)

	// EnterFor_stmt is called when entering the for_stmt production.
	EnterFor_stmt(c *For_stmtContext)

	// EnterTable_ref is called when entering the table_ref production.
	EnterTable_ref(c *Table_refContext)

	// EnterTable_key is called when entering the table_key production.
	EnterTable_key(c *Table_keyContext)

	// EnterTable_arg is called when entering the table_arg production.
	EnterTable_arg(c *Table_argContext)

	// EnterTable_hints is called when entering the table_hints production.
	EnterTable_hints(c *Table_hintsContext)

	// EnterTable_hint is called when entering the table_hint production.
	EnterTable_hint(c *Table_hintContext)

	// EnterObject_ref is called when entering the object_ref production.
	EnterObject_ref(c *Object_refContext)

	// EnterSimple_table_ref_core is called when entering the simple_table_ref_core production.
	EnterSimple_table_ref_core(c *Simple_table_ref_coreContext)

	// EnterSimple_table_ref is called when entering the simple_table_ref production.
	EnterSimple_table_ref(c *Simple_table_refContext)

	// EnterInto_simple_table_ref is called when entering the into_simple_table_ref production.
	EnterInto_simple_table_ref(c *Into_simple_table_refContext)

	// EnterDelete_stmt is called when entering the delete_stmt production.
	EnterDelete_stmt(c *Delete_stmtContext)

	// EnterUpdate_stmt is called when entering the update_stmt production.
	EnterUpdate_stmt(c *Update_stmtContext)

	// EnterSet_clause_choice is called when entering the set_clause_choice production.
	EnterSet_clause_choice(c *Set_clause_choiceContext)

	// EnterSet_clause_list is called when entering the set_clause_list production.
	EnterSet_clause_list(c *Set_clause_listContext)

	// EnterSet_clause is called when entering the set_clause production.
	EnterSet_clause(c *Set_clauseContext)

	// EnterSet_target is called when entering the set_target production.
	EnterSet_target(c *Set_targetContext)

	// EnterMultiple_column_assignment is called when entering the multiple_column_assignment production.
	EnterMultiple_column_assignment(c *Multiple_column_assignmentContext)

	// EnterSet_target_list is called when entering the set_target_list production.
	EnterSet_target_list(c *Set_target_listContext)

	// EnterCreate_topic_stmt is called when entering the create_topic_stmt production.
	EnterCreate_topic_stmt(c *Create_topic_stmtContext)

	// EnterCreate_topic_entries is called when entering the create_topic_entries production.
	EnterCreate_topic_entries(c *Create_topic_entriesContext)

	// EnterCreate_topic_entry is called when entering the create_topic_entry production.
	EnterCreate_topic_entry(c *Create_topic_entryContext)

	// EnterWith_topic_settings is called when entering the with_topic_settings production.
	EnterWith_topic_settings(c *With_topic_settingsContext)

	// EnterAlter_topic_stmt is called when entering the alter_topic_stmt production.
	EnterAlter_topic_stmt(c *Alter_topic_stmtContext)

	// EnterAlter_topic_action is called when entering the alter_topic_action production.
	EnterAlter_topic_action(c *Alter_topic_actionContext)

	// EnterAlter_topic_add_consumer is called when entering the alter_topic_add_consumer production.
	EnterAlter_topic_add_consumer(c *Alter_topic_add_consumerContext)

	// EnterTopic_create_consumer_entry is called when entering the topic_create_consumer_entry production.
	EnterTopic_create_consumer_entry(c *Topic_create_consumer_entryContext)

	// EnterAlter_topic_alter_consumer is called when entering the alter_topic_alter_consumer production.
	EnterAlter_topic_alter_consumer(c *Alter_topic_alter_consumerContext)

	// EnterAlter_topic_alter_consumer_entry is called when entering the alter_topic_alter_consumer_entry production.
	EnterAlter_topic_alter_consumer_entry(c *Alter_topic_alter_consumer_entryContext)

	// EnterAlter_topic_drop_consumer is called when entering the alter_topic_drop_consumer production.
	EnterAlter_topic_drop_consumer(c *Alter_topic_drop_consumerContext)

	// EnterTopic_alter_consumer_set is called when entering the topic_alter_consumer_set production.
	EnterTopic_alter_consumer_set(c *Topic_alter_consumer_setContext)

	// EnterTopic_alter_consumer_reset is called when entering the topic_alter_consumer_reset production.
	EnterTopic_alter_consumer_reset(c *Topic_alter_consumer_resetContext)

	// EnterAlter_topic_set_settings is called when entering the alter_topic_set_settings production.
	EnterAlter_topic_set_settings(c *Alter_topic_set_settingsContext)

	// EnterAlter_topic_reset_settings is called when entering the alter_topic_reset_settings production.
	EnterAlter_topic_reset_settings(c *Alter_topic_reset_settingsContext)

	// EnterDrop_topic_stmt is called when entering the drop_topic_stmt production.
	EnterDrop_topic_stmt(c *Drop_topic_stmtContext)

	// EnterTopic_settings is called when entering the topic_settings production.
	EnterTopic_settings(c *Topic_settingsContext)

	// EnterTopic_settings_entry is called when entering the topic_settings_entry production.
	EnterTopic_settings_entry(c *Topic_settings_entryContext)

	// EnterTopic_setting_value is called when entering the topic_setting_value production.
	EnterTopic_setting_value(c *Topic_setting_valueContext)

	// EnterTopic_consumer_with_settings is called when entering the topic_consumer_with_settings production.
	EnterTopic_consumer_with_settings(c *Topic_consumer_with_settingsContext)

	// EnterTopic_consumer_settings is called when entering the topic_consumer_settings production.
	EnterTopic_consumer_settings(c *Topic_consumer_settingsContext)

	// EnterTopic_consumer_settings_entry is called when entering the topic_consumer_settings_entry production.
	EnterTopic_consumer_settings_entry(c *Topic_consumer_settings_entryContext)

	// EnterTopic_consumer_setting_value is called when entering the topic_consumer_setting_value production.
	EnterTopic_consumer_setting_value(c *Topic_consumer_setting_valueContext)

	// EnterTopic_ref is called when entering the topic_ref production.
	EnterTopic_ref(c *Topic_refContext)

	// EnterTopic_consumer_ref is called when entering the topic_consumer_ref production.
	EnterTopic_consumer_ref(c *Topic_consumer_refContext)

	// EnterNull_treatment is called when entering the null_treatment production.
	EnterNull_treatment(c *Null_treatmentContext)

	// EnterFilter_clause is called when entering the filter_clause production.
	EnterFilter_clause(c *Filter_clauseContext)

	// EnterWindow_name_or_specification is called when entering the window_name_or_specification production.
	EnterWindow_name_or_specification(c *Window_name_or_specificationContext)

	// EnterWindow_name is called when entering the window_name production.
	EnterWindow_name(c *Window_nameContext)

	// EnterWindow_clause is called when entering the window_clause production.
	EnterWindow_clause(c *Window_clauseContext)

	// EnterWindow_definition_list is called when entering the window_definition_list production.
	EnterWindow_definition_list(c *Window_definition_listContext)

	// EnterWindow_definition is called when entering the window_definition production.
	EnterWindow_definition(c *Window_definitionContext)

	// EnterNew_window_name is called when entering the new_window_name production.
	EnterNew_window_name(c *New_window_nameContext)

	// EnterWindow_specification is called when entering the window_specification production.
	EnterWindow_specification(c *Window_specificationContext)

	// EnterWindow_specification_details is called when entering the window_specification_details production.
	EnterWindow_specification_details(c *Window_specification_detailsContext)

	// EnterExisting_window_name is called when entering the existing_window_name production.
	EnterExisting_window_name(c *Existing_window_nameContext)

	// EnterWindow_partition_clause is called when entering the window_partition_clause production.
	EnterWindow_partition_clause(c *Window_partition_clauseContext)

	// EnterWindow_order_clause is called when entering the window_order_clause production.
	EnterWindow_order_clause(c *Window_order_clauseContext)

	// EnterWindow_frame_clause is called when entering the window_frame_clause production.
	EnterWindow_frame_clause(c *Window_frame_clauseContext)

	// EnterWindow_frame_units is called when entering the window_frame_units production.
	EnterWindow_frame_units(c *Window_frame_unitsContext)

	// EnterWindow_frame_extent is called when entering the window_frame_extent production.
	EnterWindow_frame_extent(c *Window_frame_extentContext)

	// EnterWindow_frame_between is called when entering the window_frame_between production.
	EnterWindow_frame_between(c *Window_frame_betweenContext)

	// EnterWindow_frame_bound is called when entering the window_frame_bound production.
	EnterWindow_frame_bound(c *Window_frame_boundContext)

	// EnterWindow_frame_exclusion is called when entering the window_frame_exclusion production.
	EnterWindow_frame_exclusion(c *Window_frame_exclusionContext)

	// EnterUse_stmt is called when entering the use_stmt production.
	EnterUse_stmt(c *Use_stmtContext)

	// EnterSubselect_stmt is called when entering the subselect_stmt production.
	EnterSubselect_stmt(c *Subselect_stmtContext)

	// EnterNamed_nodes_stmt is called when entering the named_nodes_stmt production.
	EnterNamed_nodes_stmt(c *Named_nodes_stmtContext)

	// EnterCommit_stmt is called when entering the commit_stmt production.
	EnterCommit_stmt(c *Commit_stmtContext)

	// EnterRollback_stmt is called when entering the rollback_stmt production.
	EnterRollback_stmt(c *Rollback_stmtContext)

	// EnterAnalyze_table is called when entering the analyze_table production.
	EnterAnalyze_table(c *Analyze_tableContext)

	// EnterAnalyze_table_list is called when entering the analyze_table_list production.
	EnterAnalyze_table_list(c *Analyze_table_listContext)

	// EnterAnalyze_stmt is called when entering the analyze_stmt production.
	EnterAnalyze_stmt(c *Analyze_stmtContext)

	// EnterAlter_sequence_stmt is called when entering the alter_sequence_stmt production.
	EnterAlter_sequence_stmt(c *Alter_sequence_stmtContext)

	// EnterAlter_sequence_action is called when entering the alter_sequence_action production.
	EnterAlter_sequence_action(c *Alter_sequence_actionContext)

	// EnterIdentifier is called when entering the identifier production.
	EnterIdentifier(c *IdentifierContext)

	// EnterId is called when entering the id production.
	EnterId(c *IdContext)

	// EnterId_schema is called when entering the id_schema production.
	EnterId_schema(c *Id_schemaContext)

	// EnterId_expr is called when entering the id_expr production.
	EnterId_expr(c *Id_exprContext)

	// EnterId_expr_in is called when entering the id_expr_in production.
	EnterId_expr_in(c *Id_expr_inContext)

	// EnterId_window is called when entering the id_window production.
	EnterId_window(c *Id_windowContext)

	// EnterId_table is called when entering the id_table production.
	EnterId_table(c *Id_tableContext)

	// EnterId_without is called when entering the id_without production.
	EnterId_without(c *Id_withoutContext)

	// EnterId_hint is called when entering the id_hint production.
	EnterId_hint(c *Id_hintContext)

	// EnterId_as_compat is called when entering the id_as_compat production.
	EnterId_as_compat(c *Id_as_compatContext)

	// EnterAn_id is called when entering the an_id production.
	EnterAn_id(c *An_idContext)

	// EnterAn_id_or_type is called when entering the an_id_or_type production.
	EnterAn_id_or_type(c *An_id_or_typeContext)

	// EnterAn_id_schema is called when entering the an_id_schema production.
	EnterAn_id_schema(c *An_id_schemaContext)

	// EnterAn_id_expr is called when entering the an_id_expr production.
	EnterAn_id_expr(c *An_id_exprContext)

	// EnterAn_id_expr_in is called when entering the an_id_expr_in production.
	EnterAn_id_expr_in(c *An_id_expr_inContext)

	// EnterAn_id_window is called when entering the an_id_window production.
	EnterAn_id_window(c *An_id_windowContext)

	// EnterAn_id_table is called when entering the an_id_table production.
	EnterAn_id_table(c *An_id_tableContext)

	// EnterAn_id_without is called when entering the an_id_without production.
	EnterAn_id_without(c *An_id_withoutContext)

	// EnterAn_id_hint is called when entering the an_id_hint production.
	EnterAn_id_hint(c *An_id_hintContext)

	// EnterAn_id_pure is called when entering the an_id_pure production.
	EnterAn_id_pure(c *An_id_pureContext)

	// EnterAn_id_as_compat is called when entering the an_id_as_compat production.
	EnterAn_id_as_compat(c *An_id_as_compatContext)

	// EnterView_name is called when entering the view_name production.
	EnterView_name(c *View_nameContext)

	// EnterOpt_id_prefix is called when entering the opt_id_prefix production.
	EnterOpt_id_prefix(c *Opt_id_prefixContext)

	// EnterCluster_expr is called when entering the cluster_expr production.
	EnterCluster_expr(c *Cluster_exprContext)

	// EnterId_or_type is called when entering the id_or_type production.
	EnterId_or_type(c *Id_or_typeContext)

	// EnterOpt_id_prefix_or_type is called when entering the opt_id_prefix_or_type production.
	EnterOpt_id_prefix_or_type(c *Opt_id_prefix_or_typeContext)

	// EnterId_or_at is called when entering the id_or_at production.
	EnterId_or_at(c *Id_or_atContext)

	// EnterId_table_or_type is called when entering the id_table_or_type production.
	EnterId_table_or_type(c *Id_table_or_typeContext)

	// EnterId_table_or_at is called when entering the id_table_or_at production.
	EnterId_table_or_at(c *Id_table_or_atContext)

	// EnterKeyword is called when entering the keyword production.
	EnterKeyword(c *KeywordContext)

	// EnterKeyword_expr_uncompat is called when entering the keyword_expr_uncompat production.
	EnterKeyword_expr_uncompat(c *Keyword_expr_uncompatContext)

	// EnterKeyword_table_uncompat is called when entering the keyword_table_uncompat production.
	EnterKeyword_table_uncompat(c *Keyword_table_uncompatContext)

	// EnterKeyword_select_uncompat is called when entering the keyword_select_uncompat production.
	EnterKeyword_select_uncompat(c *Keyword_select_uncompatContext)

	// EnterKeyword_alter_uncompat is called when entering the keyword_alter_uncompat production.
	EnterKeyword_alter_uncompat(c *Keyword_alter_uncompatContext)

	// EnterKeyword_in_uncompat is called when entering the keyword_in_uncompat production.
	EnterKeyword_in_uncompat(c *Keyword_in_uncompatContext)

	// EnterKeyword_window_uncompat is called when entering the keyword_window_uncompat production.
	EnterKeyword_window_uncompat(c *Keyword_window_uncompatContext)

	// EnterKeyword_hint_uncompat is called when entering the keyword_hint_uncompat production.
	EnterKeyword_hint_uncompat(c *Keyword_hint_uncompatContext)

	// EnterKeyword_as_compat is called when entering the keyword_as_compat production.
	EnterKeyword_as_compat(c *Keyword_as_compatContext)

	// EnterKeyword_compat is called when entering the keyword_compat production.
	EnterKeyword_compat(c *Keyword_compatContext)

	// EnterType_id is called when entering the type_id production.
	EnterType_id(c *Type_idContext)

	// EnterBool_value is called when entering the bool_value production.
	EnterBool_value(c *Bool_valueContext)

	// EnterReal is called when entering the real production.
	EnterReal(c *RealContext)

	// EnterInteger is called when entering the integer production.
	EnterInteger(c *IntegerContext)

	// ExitSql_query is called when exiting the sql_query production.
	ExitSql_query(c *Sql_queryContext)

	// ExitSql_stmt_list is called when exiting the sql_stmt_list production.
	ExitSql_stmt_list(c *Sql_stmt_listContext)

	// ExitAnsi_sql_stmt_list is called when exiting the ansi_sql_stmt_list production.
	ExitAnsi_sql_stmt_list(c *Ansi_sql_stmt_listContext)

	// ExitLambda_body is called when exiting the lambda_body production.
	ExitLambda_body(c *Lambda_bodyContext)

	// ExitLambda_stmt is called when exiting the lambda_stmt production.
	ExitLambda_stmt(c *Lambda_stmtContext)

	// ExitSql_stmt is called when exiting the sql_stmt production.
	ExitSql_stmt(c *Sql_stmtContext)

	// ExitSql_stmt_core is called when exiting the sql_stmt_core production.
	ExitSql_stmt_core(c *Sql_stmt_coreContext)

	// ExitExpr is called when exiting the expr production.
	ExitExpr(c *ExprContext)

	// ExitOr_subexpr is called when exiting the or_subexpr production.
	ExitOr_subexpr(c *Or_subexprContext)

	// ExitAnd_subexpr is called when exiting the and_subexpr production.
	ExitAnd_subexpr(c *And_subexprContext)

	// ExitXor_subexpr is called when exiting the xor_subexpr production.
	ExitXor_subexpr(c *Xor_subexprContext)

	// ExitDistinct_from_op is called when exiting the distinct_from_op production.
	ExitDistinct_from_op(c *Distinct_from_opContext)

	// ExitCond_expr is called when exiting the cond_expr production.
	ExitCond_expr(c *Cond_exprContext)

	// ExitMatch_op is called when exiting the match_op production.
	ExitMatch_op(c *Match_opContext)

	// ExitEq_subexpr is called when exiting the eq_subexpr production.
	ExitEq_subexpr(c *Eq_subexprContext)

	// ExitShift_right is called when exiting the shift_right production.
	ExitShift_right(c *Shift_rightContext)

	// ExitRot_right is called when exiting the rot_right production.
	ExitRot_right(c *Rot_rightContext)

	// ExitDouble_question is called when exiting the double_question production.
	ExitDouble_question(c *Double_questionContext)

	// ExitNeq_subexpr is called when exiting the neq_subexpr production.
	ExitNeq_subexpr(c *Neq_subexprContext)

	// ExitBit_subexpr is called when exiting the bit_subexpr production.
	ExitBit_subexpr(c *Bit_subexprContext)

	// ExitAdd_subexpr is called when exiting the add_subexpr production.
	ExitAdd_subexpr(c *Add_subexprContext)

	// ExitMul_subexpr is called when exiting the mul_subexpr production.
	ExitMul_subexpr(c *Mul_subexprContext)

	// ExitCon_subexpr is called when exiting the con_subexpr production.
	ExitCon_subexpr(c *Con_subexprContext)

	// ExitUnary_op is called when exiting the unary_op production.
	ExitUnary_op(c *Unary_opContext)

	// ExitUnary_subexpr_suffix is called when exiting the unary_subexpr_suffix production.
	ExitUnary_subexpr_suffix(c *Unary_subexpr_suffixContext)

	// ExitUnary_casual_subexpr is called when exiting the unary_casual_subexpr production.
	ExitUnary_casual_subexpr(c *Unary_casual_subexprContext)

	// ExitIn_unary_casual_subexpr is called when exiting the in_unary_casual_subexpr production.
	ExitIn_unary_casual_subexpr(c *In_unary_casual_subexprContext)

	// ExitUnary_subexpr is called when exiting the unary_subexpr production.
	ExitUnary_subexpr(c *Unary_subexprContext)

	// ExitIn_unary_subexpr is called when exiting the in_unary_subexpr production.
	ExitIn_unary_subexpr(c *In_unary_subexprContext)

	// ExitList_literal is called when exiting the list_literal production.
	ExitList_literal(c *List_literalContext)

	// ExitExpr_dict_list is called when exiting the expr_dict_list production.
	ExitExpr_dict_list(c *Expr_dict_listContext)

	// ExitDict_literal is called when exiting the dict_literal production.
	ExitDict_literal(c *Dict_literalContext)

	// ExitExpr_struct_list is called when exiting the expr_struct_list production.
	ExitExpr_struct_list(c *Expr_struct_listContext)

	// ExitStruct_literal is called when exiting the struct_literal production.
	ExitStruct_literal(c *Struct_literalContext)

	// ExitAtom_expr is called when exiting the atom_expr production.
	ExitAtom_expr(c *Atom_exprContext)

	// ExitIn_atom_expr is called when exiting the in_atom_expr production.
	ExitIn_atom_expr(c *In_atom_exprContext)

	// ExitCast_expr is called when exiting the cast_expr production.
	ExitCast_expr(c *Cast_exprContext)

	// ExitBitcast_expr is called when exiting the bitcast_expr production.
	ExitBitcast_expr(c *Bitcast_exprContext)

	// ExitExists_expr is called when exiting the exists_expr production.
	ExitExists_expr(c *Exists_exprContext)

	// ExitCase_expr is called when exiting the case_expr production.
	ExitCase_expr(c *Case_exprContext)

	// ExitLambda is called when exiting the lambda production.
	ExitLambda(c *LambdaContext)

	// ExitIn_expr is called when exiting the in_expr production.
	ExitIn_expr(c *In_exprContext)

	// ExitJson_api_expr is called when exiting the json_api_expr production.
	ExitJson_api_expr(c *Json_api_exprContext)

	// ExitJsonpath_spec is called when exiting the jsonpath_spec production.
	ExitJsonpath_spec(c *Jsonpath_specContext)

	// ExitJson_variable_name is called when exiting the json_variable_name production.
	ExitJson_variable_name(c *Json_variable_nameContext)

	// ExitJson_variable is called when exiting the json_variable production.
	ExitJson_variable(c *Json_variableContext)

	// ExitJson_variables is called when exiting the json_variables production.
	ExitJson_variables(c *Json_variablesContext)

	// ExitJson_common_args is called when exiting the json_common_args production.
	ExitJson_common_args(c *Json_common_argsContext)

	// ExitJson_case_handler is called when exiting the json_case_handler production.
	ExitJson_case_handler(c *Json_case_handlerContext)

	// ExitJson_value is called when exiting the json_value production.
	ExitJson_value(c *Json_valueContext)

	// ExitJson_exists_handler is called when exiting the json_exists_handler production.
	ExitJson_exists_handler(c *Json_exists_handlerContext)

	// ExitJson_exists is called when exiting the json_exists production.
	ExitJson_exists(c *Json_existsContext)

	// ExitJson_query_wrapper is called when exiting the json_query_wrapper production.
	ExitJson_query_wrapper(c *Json_query_wrapperContext)

	// ExitJson_query_handler is called when exiting the json_query_handler production.
	ExitJson_query_handler(c *Json_query_handlerContext)

	// ExitJson_query is called when exiting the json_query production.
	ExitJson_query(c *Json_queryContext)

	// ExitSmart_parenthesis is called when exiting the smart_parenthesis production.
	ExitSmart_parenthesis(c *Smart_parenthesisContext)

	// ExitExpr_list is called when exiting the expr_list production.
	ExitExpr_list(c *Expr_listContext)

	// ExitPure_column_list is called when exiting the pure_column_list production.
	ExitPure_column_list(c *Pure_column_listContext)

	// ExitPure_column_or_named is called when exiting the pure_column_or_named production.
	ExitPure_column_or_named(c *Pure_column_or_namedContext)

	// ExitPure_column_or_named_list is called when exiting the pure_column_or_named_list production.
	ExitPure_column_or_named_list(c *Pure_column_or_named_listContext)

	// ExitColumn_name is called when exiting the column_name production.
	ExitColumn_name(c *Column_nameContext)

	// ExitWithout_column_name is called when exiting the without_column_name production.
	ExitWithout_column_name(c *Without_column_nameContext)

	// ExitColumn_list is called when exiting the column_list production.
	ExitColumn_list(c *Column_listContext)

	// ExitWithout_column_list is called when exiting the without_column_list production.
	ExitWithout_column_list(c *Without_column_listContext)

	// ExitNamed_expr is called when exiting the named_expr production.
	ExitNamed_expr(c *Named_exprContext)

	// ExitNamed_expr_list is called when exiting the named_expr_list production.
	ExitNamed_expr_list(c *Named_expr_listContext)

	// ExitInvoke_expr is called when exiting the invoke_expr production.
	ExitInvoke_expr(c *Invoke_exprContext)

	// ExitInvoke_expr_tail is called when exiting the invoke_expr_tail production.
	ExitInvoke_expr_tail(c *Invoke_expr_tailContext)

	// ExitUsing_call_expr is called when exiting the using_call_expr production.
	ExitUsing_call_expr(c *Using_call_exprContext)

	// ExitKey_expr is called when exiting the key_expr production.
	ExitKey_expr(c *Key_exprContext)

	// ExitWhen_expr is called when exiting the when_expr production.
	ExitWhen_expr(c *When_exprContext)

	// ExitLiteral_value is called when exiting the literal_value production.
	ExitLiteral_value(c *Literal_valueContext)

	// ExitBind_parameter is called when exiting the bind_parameter production.
	ExitBind_parameter(c *Bind_parameterContext)

	// ExitOpt_bind_parameter is called when exiting the opt_bind_parameter production.
	ExitOpt_bind_parameter(c *Opt_bind_parameterContext)

	// ExitBind_parameter_list is called when exiting the bind_parameter_list production.
	ExitBind_parameter_list(c *Bind_parameter_listContext)

	// ExitNamed_bind_parameter is called when exiting the named_bind_parameter production.
	ExitNamed_bind_parameter(c *Named_bind_parameterContext)

	// ExitNamed_bind_parameter_list is called when exiting the named_bind_parameter_list production.
	ExitNamed_bind_parameter_list(c *Named_bind_parameter_listContext)

	// ExitSigned_number is called when exiting the signed_number production.
	ExitSigned_number(c *Signed_numberContext)

	// ExitType_name_simple is called when exiting the type_name_simple production.
	ExitType_name_simple(c *Type_name_simpleContext)

	// ExitInteger_or_bind is called when exiting the integer_or_bind production.
	ExitInteger_or_bind(c *Integer_or_bindContext)

	// ExitType_name_tag is called when exiting the type_name_tag production.
	ExitType_name_tag(c *Type_name_tagContext)

	// ExitStruct_arg is called when exiting the struct_arg production.
	ExitStruct_arg(c *Struct_argContext)

	// ExitStruct_arg_positional is called when exiting the struct_arg_positional production.
	ExitStruct_arg_positional(c *Struct_arg_positionalContext)

	// ExitVariant_arg is called when exiting the variant_arg production.
	ExitVariant_arg(c *Variant_argContext)

	// ExitCallable_arg is called when exiting the callable_arg production.
	ExitCallable_arg(c *Callable_argContext)

	// ExitCallable_arg_list is called when exiting the callable_arg_list production.
	ExitCallable_arg_list(c *Callable_arg_listContext)

	// ExitType_name_decimal is called when exiting the type_name_decimal production.
	ExitType_name_decimal(c *Type_name_decimalContext)

	// ExitType_name_optional is called when exiting the type_name_optional production.
	ExitType_name_optional(c *Type_name_optionalContext)

	// ExitType_name_tuple is called when exiting the type_name_tuple production.
	ExitType_name_tuple(c *Type_name_tupleContext)

	// ExitType_name_struct is called when exiting the type_name_struct production.
	ExitType_name_struct(c *Type_name_structContext)

	// ExitType_name_variant is called when exiting the type_name_variant production.
	ExitType_name_variant(c *Type_name_variantContext)

	// ExitType_name_list is called when exiting the type_name_list production.
	ExitType_name_list(c *Type_name_listContext)

	// ExitType_name_stream is called when exiting the type_name_stream production.
	ExitType_name_stream(c *Type_name_streamContext)

	// ExitType_name_flow is called when exiting the type_name_flow production.
	ExitType_name_flow(c *Type_name_flowContext)

	// ExitType_name_dict is called when exiting the type_name_dict production.
	ExitType_name_dict(c *Type_name_dictContext)

	// ExitType_name_set is called when exiting the type_name_set production.
	ExitType_name_set(c *Type_name_setContext)

	// ExitType_name_enum is called when exiting the type_name_enum production.
	ExitType_name_enum(c *Type_name_enumContext)

	// ExitType_name_resource is called when exiting the type_name_resource production.
	ExitType_name_resource(c *Type_name_resourceContext)

	// ExitType_name_tagged is called when exiting the type_name_tagged production.
	ExitType_name_tagged(c *Type_name_taggedContext)

	// ExitType_name_callable is called when exiting the type_name_callable production.
	ExitType_name_callable(c *Type_name_callableContext)

	// ExitType_name_composite is called when exiting the type_name_composite production.
	ExitType_name_composite(c *Type_name_compositeContext)

	// ExitType_name is called when exiting the type_name production.
	ExitType_name(c *Type_nameContext)

	// ExitType_name_or_bind is called when exiting the type_name_or_bind production.
	ExitType_name_or_bind(c *Type_name_or_bindContext)

	// ExitValue_constructor_literal is called when exiting the value_constructor_literal production.
	ExitValue_constructor_literal(c *Value_constructor_literalContext)

	// ExitValue_constructor is called when exiting the value_constructor production.
	ExitValue_constructor(c *Value_constructorContext)

	// ExitDeclare_stmt is called when exiting the declare_stmt production.
	ExitDeclare_stmt(c *Declare_stmtContext)

	// ExitModule_path is called when exiting the module_path production.
	ExitModule_path(c *Module_pathContext)

	// ExitImport_stmt is called when exiting the import_stmt production.
	ExitImport_stmt(c *Import_stmtContext)

	// ExitExport_stmt is called when exiting the export_stmt production.
	ExitExport_stmt(c *Export_stmtContext)

	// ExitCall_action is called when exiting the call_action production.
	ExitCall_action(c *Call_actionContext)

	// ExitInline_action is called when exiting the inline_action production.
	ExitInline_action(c *Inline_actionContext)

	// ExitDo_stmt is called when exiting the do_stmt production.
	ExitDo_stmt(c *Do_stmtContext)

	// ExitPragma_stmt is called when exiting the pragma_stmt production.
	ExitPragma_stmt(c *Pragma_stmtContext)

	// ExitPragma_value is called when exiting the pragma_value production.
	ExitPragma_value(c *Pragma_valueContext)

	// ExitSort_specification is called when exiting the sort_specification production.
	ExitSort_specification(c *Sort_specificationContext)

	// ExitSort_specification_list is called when exiting the sort_specification_list production.
	ExitSort_specification_list(c *Sort_specification_listContext)

	// ExitSelect_stmt is called when exiting the select_stmt production.
	ExitSelect_stmt(c *Select_stmtContext)

	// ExitSelect_unparenthesized_stmt is called when exiting the select_unparenthesized_stmt production.
	ExitSelect_unparenthesized_stmt(c *Select_unparenthesized_stmtContext)

	// ExitSelect_kind_parenthesis is called when exiting the select_kind_parenthesis production.
	ExitSelect_kind_parenthesis(c *Select_kind_parenthesisContext)

	// ExitSelect_op is called when exiting the select_op production.
	ExitSelect_op(c *Select_opContext)

	// ExitSelect_kind_partial is called when exiting the select_kind_partial production.
	ExitSelect_kind_partial(c *Select_kind_partialContext)

	// ExitSelect_kind is called when exiting the select_kind production.
	ExitSelect_kind(c *Select_kindContext)

	// ExitProcess_core is called when exiting the process_core production.
	ExitProcess_core(c *Process_coreContext)

	// ExitExternal_call_param is called when exiting the external_call_param production.
	ExitExternal_call_param(c *External_call_paramContext)

	// ExitExternal_call_settings is called when exiting the external_call_settings production.
	ExitExternal_call_settings(c *External_call_settingsContext)

	// ExitReduce_core is called when exiting the reduce_core production.
	ExitReduce_core(c *Reduce_coreContext)

	// ExitOpt_set_quantifier is called when exiting the opt_set_quantifier production.
	ExitOpt_set_quantifier(c *Opt_set_quantifierContext)

	// ExitSelect_core is called when exiting the select_core production.
	ExitSelect_core(c *Select_coreContext)

	// ExitRow_pattern_recognition_clause is called when exiting the row_pattern_recognition_clause production.
	ExitRow_pattern_recognition_clause(c *Row_pattern_recognition_clauseContext)

	// ExitRow_pattern_rows_per_match is called when exiting the row_pattern_rows_per_match production.
	ExitRow_pattern_rows_per_match(c *Row_pattern_rows_per_matchContext)

	// ExitRow_pattern_empty_match_handling is called when exiting the row_pattern_empty_match_handling production.
	ExitRow_pattern_empty_match_handling(c *Row_pattern_empty_match_handlingContext)

	// ExitRow_pattern_measures is called when exiting the row_pattern_measures production.
	ExitRow_pattern_measures(c *Row_pattern_measuresContext)

	// ExitRow_pattern_measure_list is called when exiting the row_pattern_measure_list production.
	ExitRow_pattern_measure_list(c *Row_pattern_measure_listContext)

	// ExitRow_pattern_measure_definition is called when exiting the row_pattern_measure_definition production.
	ExitRow_pattern_measure_definition(c *Row_pattern_measure_definitionContext)

	// ExitRow_pattern_common_syntax is called when exiting the row_pattern_common_syntax production.
	ExitRow_pattern_common_syntax(c *Row_pattern_common_syntaxContext)

	// ExitRow_pattern_skip_to is called when exiting the row_pattern_skip_to production.
	ExitRow_pattern_skip_to(c *Row_pattern_skip_toContext)

	// ExitRow_pattern_skip_to_variable_name is called when exiting the row_pattern_skip_to_variable_name production.
	ExitRow_pattern_skip_to_variable_name(c *Row_pattern_skip_to_variable_nameContext)

	// ExitRow_pattern_initial_or_seek is called when exiting the row_pattern_initial_or_seek production.
	ExitRow_pattern_initial_or_seek(c *Row_pattern_initial_or_seekContext)

	// ExitRow_pattern is called when exiting the row_pattern production.
	ExitRow_pattern(c *Row_patternContext)

	// ExitRow_pattern_term is called when exiting the row_pattern_term production.
	ExitRow_pattern_term(c *Row_pattern_termContext)

	// ExitRow_pattern_factor is called when exiting the row_pattern_factor production.
	ExitRow_pattern_factor(c *Row_pattern_factorContext)

	// ExitRow_pattern_quantifier is called when exiting the row_pattern_quantifier production.
	ExitRow_pattern_quantifier(c *Row_pattern_quantifierContext)

	// ExitRow_pattern_primary is called when exiting the row_pattern_primary production.
	ExitRow_pattern_primary(c *Row_pattern_primaryContext)

	// ExitRow_pattern_primary_variable_name is called when exiting the row_pattern_primary_variable_name production.
	ExitRow_pattern_primary_variable_name(c *Row_pattern_primary_variable_nameContext)

	// ExitRow_pattern_permute is called when exiting the row_pattern_permute production.
	ExitRow_pattern_permute(c *Row_pattern_permuteContext)

	// ExitRow_pattern_subset_clause is called when exiting the row_pattern_subset_clause production.
	ExitRow_pattern_subset_clause(c *Row_pattern_subset_clauseContext)

	// ExitRow_pattern_subset_list is called when exiting the row_pattern_subset_list production.
	ExitRow_pattern_subset_list(c *Row_pattern_subset_listContext)

	// ExitRow_pattern_subset_item is called when exiting the row_pattern_subset_item production.
	ExitRow_pattern_subset_item(c *Row_pattern_subset_itemContext)

	// ExitRow_pattern_subset_item_variable_name is called when exiting the row_pattern_subset_item_variable_name production.
	ExitRow_pattern_subset_item_variable_name(c *Row_pattern_subset_item_variable_nameContext)

	// ExitRow_pattern_subset_rhs is called when exiting the row_pattern_subset_rhs production.
	ExitRow_pattern_subset_rhs(c *Row_pattern_subset_rhsContext)

	// ExitRow_pattern_subset_rhs_variable_name is called when exiting the row_pattern_subset_rhs_variable_name production.
	ExitRow_pattern_subset_rhs_variable_name(c *Row_pattern_subset_rhs_variable_nameContext)

	// ExitRow_pattern_definition_list is called when exiting the row_pattern_definition_list production.
	ExitRow_pattern_definition_list(c *Row_pattern_definition_listContext)

	// ExitRow_pattern_definition is called when exiting the row_pattern_definition production.
	ExitRow_pattern_definition(c *Row_pattern_definitionContext)

	// ExitRow_pattern_definition_variable_name is called when exiting the row_pattern_definition_variable_name production.
	ExitRow_pattern_definition_variable_name(c *Row_pattern_definition_variable_nameContext)

	// ExitRow_pattern_definition_search_condition is called when exiting the row_pattern_definition_search_condition production.
	ExitRow_pattern_definition_search_condition(c *Row_pattern_definition_search_conditionContext)

	// ExitSearch_condition is called when exiting the search_condition production.
	ExitSearch_condition(c *Search_conditionContext)

	// ExitRow_pattern_variable_name is called when exiting the row_pattern_variable_name production.
	ExitRow_pattern_variable_name(c *Row_pattern_variable_nameContext)

	// ExitOrder_by_clause is called when exiting the order_by_clause production.
	ExitOrder_by_clause(c *Order_by_clauseContext)

	// ExitExt_order_by_clause is called when exiting the ext_order_by_clause production.
	ExitExt_order_by_clause(c *Ext_order_by_clauseContext)

	// ExitGroup_by_clause is called when exiting the group_by_clause production.
	ExitGroup_by_clause(c *Group_by_clauseContext)

	// ExitGrouping_element_list is called when exiting the grouping_element_list production.
	ExitGrouping_element_list(c *Grouping_element_listContext)

	// ExitGrouping_element is called when exiting the grouping_element production.
	ExitGrouping_element(c *Grouping_elementContext)

	// ExitOrdinary_grouping_set is called when exiting the ordinary_grouping_set production.
	ExitOrdinary_grouping_set(c *Ordinary_grouping_setContext)

	// ExitOrdinary_grouping_set_list is called when exiting the ordinary_grouping_set_list production.
	ExitOrdinary_grouping_set_list(c *Ordinary_grouping_set_listContext)

	// ExitRollup_list is called when exiting the rollup_list production.
	ExitRollup_list(c *Rollup_listContext)

	// ExitCube_list is called when exiting the cube_list production.
	ExitCube_list(c *Cube_listContext)

	// ExitGrouping_sets_specification is called when exiting the grouping_sets_specification production.
	ExitGrouping_sets_specification(c *Grouping_sets_specificationContext)

	// ExitHopping_window_specification is called when exiting the hopping_window_specification production.
	ExitHopping_window_specification(c *Hopping_window_specificationContext)

	// ExitResult_column is called when exiting the result_column production.
	ExitResult_column(c *Result_columnContext)

	// ExitJoin_source is called when exiting the join_source production.
	ExitJoin_source(c *Join_sourceContext)

	// ExitNamed_column is called when exiting the named_column production.
	ExitNamed_column(c *Named_columnContext)

	// ExitFlatten_by_arg is called when exiting the flatten_by_arg production.
	ExitFlatten_by_arg(c *Flatten_by_argContext)

	// ExitFlatten_source is called when exiting the flatten_source production.
	ExitFlatten_source(c *Flatten_sourceContext)

	// ExitNamed_single_source is called when exiting the named_single_source production.
	ExitNamed_single_source(c *Named_single_sourceContext)

	// ExitSingle_source is called when exiting the single_source production.
	ExitSingle_source(c *Single_sourceContext)

	// ExitSample_clause is called when exiting the sample_clause production.
	ExitSample_clause(c *Sample_clauseContext)

	// ExitTablesample_clause is called when exiting the tablesample_clause production.
	ExitTablesample_clause(c *Tablesample_clauseContext)

	// ExitSampling_mode is called when exiting the sampling_mode production.
	ExitSampling_mode(c *Sampling_modeContext)

	// ExitRepeatable_clause is called when exiting the repeatable_clause production.
	ExitRepeatable_clause(c *Repeatable_clauseContext)

	// ExitJoin_op is called when exiting the join_op production.
	ExitJoin_op(c *Join_opContext)

	// ExitJoin_constraint is called when exiting the join_constraint production.
	ExitJoin_constraint(c *Join_constraintContext)

	// ExitReturning_columns_list is called when exiting the returning_columns_list production.
	ExitReturning_columns_list(c *Returning_columns_listContext)

	// ExitInto_table_stmt is called when exiting the into_table_stmt production.
	ExitInto_table_stmt(c *Into_table_stmtContext)

	// ExitInto_values_source is called when exiting the into_values_source production.
	ExitInto_values_source(c *Into_values_sourceContext)

	// ExitValues_stmt is called when exiting the values_stmt production.
	ExitValues_stmt(c *Values_stmtContext)

	// ExitValues_source is called when exiting the values_source production.
	ExitValues_source(c *Values_sourceContext)

	// ExitValues_source_row_list is called when exiting the values_source_row_list production.
	ExitValues_source_row_list(c *Values_source_row_listContext)

	// ExitValues_source_row is called when exiting the values_source_row production.
	ExitValues_source_row(c *Values_source_rowContext)

	// ExitSimple_values_source is called when exiting the simple_values_source production.
	ExitSimple_values_source(c *Simple_values_sourceContext)

	// ExitCreate_external_data_source_stmt is called when exiting the create_external_data_source_stmt production.
	ExitCreate_external_data_source_stmt(c *Create_external_data_source_stmtContext)

	// ExitAlter_external_data_source_stmt is called when exiting the alter_external_data_source_stmt production.
	ExitAlter_external_data_source_stmt(c *Alter_external_data_source_stmtContext)

	// ExitAlter_external_data_source_action is called when exiting the alter_external_data_source_action production.
	ExitAlter_external_data_source_action(c *Alter_external_data_source_actionContext)

	// ExitDrop_external_data_source_stmt is called when exiting the drop_external_data_source_stmt production.
	ExitDrop_external_data_source_stmt(c *Drop_external_data_source_stmtContext)

	// ExitCreate_view_stmt is called when exiting the create_view_stmt production.
	ExitCreate_view_stmt(c *Create_view_stmtContext)

	// ExitDrop_view_stmt is called when exiting the drop_view_stmt production.
	ExitDrop_view_stmt(c *Drop_view_stmtContext)

	// ExitUpsert_object_stmt is called when exiting the upsert_object_stmt production.
	ExitUpsert_object_stmt(c *Upsert_object_stmtContext)

	// ExitCreate_object_stmt is called when exiting the create_object_stmt production.
	ExitCreate_object_stmt(c *Create_object_stmtContext)

	// ExitCreate_object_features is called when exiting the create_object_features production.
	ExitCreate_object_features(c *Create_object_featuresContext)

	// ExitAlter_object_stmt is called when exiting the alter_object_stmt production.
	ExitAlter_object_stmt(c *Alter_object_stmtContext)

	// ExitAlter_object_features is called when exiting the alter_object_features production.
	ExitAlter_object_features(c *Alter_object_featuresContext)

	// ExitDrop_object_stmt is called when exiting the drop_object_stmt production.
	ExitDrop_object_stmt(c *Drop_object_stmtContext)

	// ExitDrop_object_features is called when exiting the drop_object_features production.
	ExitDrop_object_features(c *Drop_object_featuresContext)

	// ExitObject_feature_value is called when exiting the object_feature_value production.
	ExitObject_feature_value(c *Object_feature_valueContext)

	// ExitObject_feature_kv is called when exiting the object_feature_kv production.
	ExitObject_feature_kv(c *Object_feature_kvContext)

	// ExitObject_feature_flag is called when exiting the object_feature_flag production.
	ExitObject_feature_flag(c *Object_feature_flagContext)

	// ExitObject_feature is called when exiting the object_feature production.
	ExitObject_feature(c *Object_featureContext)

	// ExitObject_features is called when exiting the object_features production.
	ExitObject_features(c *Object_featuresContext)

	// ExitObject_type_ref is called when exiting the object_type_ref production.
	ExitObject_type_ref(c *Object_type_refContext)

	// ExitCreate_table_stmt is called when exiting the create_table_stmt production.
	ExitCreate_table_stmt(c *Create_table_stmtContext)

	// ExitCreate_table_entry is called when exiting the create_table_entry production.
	ExitCreate_table_entry(c *Create_table_entryContext)

	// ExitCreate_backup_collection_stmt is called when exiting the create_backup_collection_stmt production.
	ExitCreate_backup_collection_stmt(c *Create_backup_collection_stmtContext)

	// ExitAlter_backup_collection_stmt is called when exiting the alter_backup_collection_stmt production.
	ExitAlter_backup_collection_stmt(c *Alter_backup_collection_stmtContext)

	// ExitDrop_backup_collection_stmt is called when exiting the drop_backup_collection_stmt production.
	ExitDrop_backup_collection_stmt(c *Drop_backup_collection_stmtContext)

	// ExitCreate_backup_collection_entries is called when exiting the create_backup_collection_entries production.
	ExitCreate_backup_collection_entries(c *Create_backup_collection_entriesContext)

	// ExitCreate_backup_collection_entries_many is called when exiting the create_backup_collection_entries_many production.
	ExitCreate_backup_collection_entries_many(c *Create_backup_collection_entries_manyContext)

	// ExitTable_list is called when exiting the table_list production.
	ExitTable_list(c *Table_listContext)

	// ExitAlter_backup_collection_actions is called when exiting the alter_backup_collection_actions production.
	ExitAlter_backup_collection_actions(c *Alter_backup_collection_actionsContext)

	// ExitAlter_backup_collection_action is called when exiting the alter_backup_collection_action production.
	ExitAlter_backup_collection_action(c *Alter_backup_collection_actionContext)

	// ExitAlter_backup_collection_entries is called when exiting the alter_backup_collection_entries production.
	ExitAlter_backup_collection_entries(c *Alter_backup_collection_entriesContext)

	// ExitAlter_backup_collection_entry is called when exiting the alter_backup_collection_entry production.
	ExitAlter_backup_collection_entry(c *Alter_backup_collection_entryContext)

	// ExitBackup_collection is called when exiting the backup_collection production.
	ExitBackup_collection(c *Backup_collectionContext)

	// ExitBackup_collection_settings is called when exiting the backup_collection_settings production.
	ExitBackup_collection_settings(c *Backup_collection_settingsContext)

	// ExitBackup_collection_settings_entry is called when exiting the backup_collection_settings_entry production.
	ExitBackup_collection_settings_entry(c *Backup_collection_settings_entryContext)

	// ExitBackup_stmt is called when exiting the backup_stmt production.
	ExitBackup_stmt(c *Backup_stmtContext)

	// ExitRestore_stmt is called when exiting the restore_stmt production.
	ExitRestore_stmt(c *Restore_stmtContext)

	// ExitTable_inherits is called when exiting the table_inherits production.
	ExitTable_inherits(c *Table_inheritsContext)

	// ExitTable_partition_by is called when exiting the table_partition_by production.
	ExitTable_partition_by(c *Table_partition_byContext)

	// ExitWith_table_settings is called when exiting the with_table_settings production.
	ExitWith_table_settings(c *With_table_settingsContext)

	// ExitTable_tablestore is called when exiting the table_tablestore production.
	ExitTable_tablestore(c *Table_tablestoreContext)

	// ExitTable_settings_entry is called when exiting the table_settings_entry production.
	ExitTable_settings_entry(c *Table_settings_entryContext)

	// ExitTable_as_source is called when exiting the table_as_source production.
	ExitTable_as_source(c *Table_as_sourceContext)

	// ExitAlter_table_stmt is called when exiting the alter_table_stmt production.
	ExitAlter_table_stmt(c *Alter_table_stmtContext)

	// ExitAlter_table_action is called when exiting the alter_table_action production.
	ExitAlter_table_action(c *Alter_table_actionContext)

	// ExitAlter_external_table_stmt is called when exiting the alter_external_table_stmt production.
	ExitAlter_external_table_stmt(c *Alter_external_table_stmtContext)

	// ExitAlter_external_table_action is called when exiting the alter_external_table_action production.
	ExitAlter_external_table_action(c *Alter_external_table_actionContext)

	// ExitAlter_table_store_stmt is called when exiting the alter_table_store_stmt production.
	ExitAlter_table_store_stmt(c *Alter_table_store_stmtContext)

	// ExitAlter_table_store_action is called when exiting the alter_table_store_action production.
	ExitAlter_table_store_action(c *Alter_table_store_actionContext)

	// ExitAlter_table_add_column is called when exiting the alter_table_add_column production.
	ExitAlter_table_add_column(c *Alter_table_add_columnContext)

	// ExitAlter_table_drop_column is called when exiting the alter_table_drop_column production.
	ExitAlter_table_drop_column(c *Alter_table_drop_columnContext)

	// ExitAlter_table_alter_column is called when exiting the alter_table_alter_column production.
	ExitAlter_table_alter_column(c *Alter_table_alter_columnContext)

	// ExitAlter_table_alter_column_drop_not_null is called when exiting the alter_table_alter_column_drop_not_null production.
	ExitAlter_table_alter_column_drop_not_null(c *Alter_table_alter_column_drop_not_nullContext)

	// ExitAlter_table_add_column_family is called when exiting the alter_table_add_column_family production.
	ExitAlter_table_add_column_family(c *Alter_table_add_column_familyContext)

	// ExitAlter_table_alter_column_family is called when exiting the alter_table_alter_column_family production.
	ExitAlter_table_alter_column_family(c *Alter_table_alter_column_familyContext)

	// ExitAlter_table_set_table_setting_uncompat is called when exiting the alter_table_set_table_setting_uncompat production.
	ExitAlter_table_set_table_setting_uncompat(c *Alter_table_set_table_setting_uncompatContext)

	// ExitAlter_table_set_table_setting_compat is called when exiting the alter_table_set_table_setting_compat production.
	ExitAlter_table_set_table_setting_compat(c *Alter_table_set_table_setting_compatContext)

	// ExitAlter_table_reset_table_setting is called when exiting the alter_table_reset_table_setting production.
	ExitAlter_table_reset_table_setting(c *Alter_table_reset_table_settingContext)

	// ExitAlter_table_add_index is called when exiting the alter_table_add_index production.
	ExitAlter_table_add_index(c *Alter_table_add_indexContext)

	// ExitAlter_table_drop_index is called when exiting the alter_table_drop_index production.
	ExitAlter_table_drop_index(c *Alter_table_drop_indexContext)

	// ExitAlter_table_rename_to is called when exiting the alter_table_rename_to production.
	ExitAlter_table_rename_to(c *Alter_table_rename_toContext)

	// ExitAlter_table_rename_index_to is called when exiting the alter_table_rename_index_to production.
	ExitAlter_table_rename_index_to(c *Alter_table_rename_index_toContext)

	// ExitAlter_table_add_changefeed is called when exiting the alter_table_add_changefeed production.
	ExitAlter_table_add_changefeed(c *Alter_table_add_changefeedContext)

	// ExitAlter_table_alter_changefeed is called when exiting the alter_table_alter_changefeed production.
	ExitAlter_table_alter_changefeed(c *Alter_table_alter_changefeedContext)

	// ExitAlter_table_drop_changefeed is called when exiting the alter_table_drop_changefeed production.
	ExitAlter_table_drop_changefeed(c *Alter_table_drop_changefeedContext)

	// ExitAlter_table_alter_index is called when exiting the alter_table_alter_index production.
	ExitAlter_table_alter_index(c *Alter_table_alter_indexContext)

	// ExitColumn_schema is called when exiting the column_schema production.
	ExitColumn_schema(c *Column_schemaContext)

	// ExitFamily_relation is called when exiting the family_relation production.
	ExitFamily_relation(c *Family_relationContext)

	// ExitOpt_column_constraints is called when exiting the opt_column_constraints production.
	ExitOpt_column_constraints(c *Opt_column_constraintsContext)

	// ExitColumn_order_by_specification is called when exiting the column_order_by_specification production.
	ExitColumn_order_by_specification(c *Column_order_by_specificationContext)

	// ExitTable_constraint is called when exiting the table_constraint production.
	ExitTable_constraint(c *Table_constraintContext)

	// ExitTable_index is called when exiting the table_index production.
	ExitTable_index(c *Table_indexContext)

	// ExitTable_index_type is called when exiting the table_index_type production.
	ExitTable_index_type(c *Table_index_typeContext)

	// ExitGlobal_index is called when exiting the global_index production.
	ExitGlobal_index(c *Global_indexContext)

	// ExitLocal_index is called when exiting the local_index production.
	ExitLocal_index(c *Local_indexContext)

	// ExitIndex_subtype is called when exiting the index_subtype production.
	ExitIndex_subtype(c *Index_subtypeContext)

	// ExitWith_index_settings is called when exiting the with_index_settings production.
	ExitWith_index_settings(c *With_index_settingsContext)

	// ExitIndex_setting_entry is called when exiting the index_setting_entry production.
	ExitIndex_setting_entry(c *Index_setting_entryContext)

	// ExitIndex_setting_value is called when exiting the index_setting_value production.
	ExitIndex_setting_value(c *Index_setting_valueContext)

	// ExitChangefeed is called when exiting the changefeed production.
	ExitChangefeed(c *ChangefeedContext)

	// ExitChangefeed_settings is called when exiting the changefeed_settings production.
	ExitChangefeed_settings(c *Changefeed_settingsContext)

	// ExitChangefeed_settings_entry is called when exiting the changefeed_settings_entry production.
	ExitChangefeed_settings_entry(c *Changefeed_settings_entryContext)

	// ExitChangefeed_setting_value is called when exiting the changefeed_setting_value production.
	ExitChangefeed_setting_value(c *Changefeed_setting_valueContext)

	// ExitChangefeed_alter_settings is called when exiting the changefeed_alter_settings production.
	ExitChangefeed_alter_settings(c *Changefeed_alter_settingsContext)

	// ExitAlter_table_setting_entry is called when exiting the alter_table_setting_entry production.
	ExitAlter_table_setting_entry(c *Alter_table_setting_entryContext)

	// ExitTable_setting_value is called when exiting the table_setting_value production.
	ExitTable_setting_value(c *Table_setting_valueContext)

	// ExitTtl_tier_list is called when exiting the ttl_tier_list production.
	ExitTtl_tier_list(c *Ttl_tier_listContext)

	// ExitTtl_tier_action is called when exiting the ttl_tier_action production.
	ExitTtl_tier_action(c *Ttl_tier_actionContext)

	// ExitFamily_entry is called when exiting the family_entry production.
	ExitFamily_entry(c *Family_entryContext)

	// ExitFamily_settings is called when exiting the family_settings production.
	ExitFamily_settings(c *Family_settingsContext)

	// ExitFamily_settings_entry is called when exiting the family_settings_entry production.
	ExitFamily_settings_entry(c *Family_settings_entryContext)

	// ExitFamily_setting_value is called when exiting the family_setting_value production.
	ExitFamily_setting_value(c *Family_setting_valueContext)

	// ExitSplit_boundaries is called when exiting the split_boundaries production.
	ExitSplit_boundaries(c *Split_boundariesContext)

	// ExitLiteral_value_list is called when exiting the literal_value_list production.
	ExitLiteral_value_list(c *Literal_value_listContext)

	// ExitAlter_table_alter_index_action is called when exiting the alter_table_alter_index_action production.
	ExitAlter_table_alter_index_action(c *Alter_table_alter_index_actionContext)

	// ExitDrop_table_stmt is called when exiting the drop_table_stmt production.
	ExitDrop_table_stmt(c *Drop_table_stmtContext)

	// ExitCreate_user_stmt is called when exiting the create_user_stmt production.
	ExitCreate_user_stmt(c *Create_user_stmtContext)

	// ExitAlter_user_stmt is called when exiting the alter_user_stmt production.
	ExitAlter_user_stmt(c *Alter_user_stmtContext)

	// ExitCreate_group_stmt is called when exiting the create_group_stmt production.
	ExitCreate_group_stmt(c *Create_group_stmtContext)

	// ExitAlter_group_stmt is called when exiting the alter_group_stmt production.
	ExitAlter_group_stmt(c *Alter_group_stmtContext)

	// ExitDrop_role_stmt is called when exiting the drop_role_stmt production.
	ExitDrop_role_stmt(c *Drop_role_stmtContext)

	// ExitRole_name is called when exiting the role_name production.
	ExitRole_name(c *Role_nameContext)

	// ExitCreate_user_option is called when exiting the create_user_option production.
	ExitCreate_user_option(c *Create_user_optionContext)

	// ExitPassword_option is called when exiting the password_option production.
	ExitPassword_option(c *Password_optionContext)

	// ExitLogin_option is called when exiting the login_option production.
	ExitLogin_option(c *Login_optionContext)

	// ExitGrant_permissions_stmt is called when exiting the grant_permissions_stmt production.
	ExitGrant_permissions_stmt(c *Grant_permissions_stmtContext)

	// ExitRevoke_permissions_stmt is called when exiting the revoke_permissions_stmt production.
	ExitRevoke_permissions_stmt(c *Revoke_permissions_stmtContext)

	// ExitPermission_id is called when exiting the permission_id production.
	ExitPermission_id(c *Permission_idContext)

	// ExitPermission_name is called when exiting the permission_name production.
	ExitPermission_name(c *Permission_nameContext)

	// ExitPermission_name_target is called when exiting the permission_name_target production.
	ExitPermission_name_target(c *Permission_name_targetContext)

	// ExitCreate_resource_pool_stmt is called when exiting the create_resource_pool_stmt production.
	ExitCreate_resource_pool_stmt(c *Create_resource_pool_stmtContext)

	// ExitAlter_resource_pool_stmt is called when exiting the alter_resource_pool_stmt production.
	ExitAlter_resource_pool_stmt(c *Alter_resource_pool_stmtContext)

	// ExitAlter_resource_pool_action is called when exiting the alter_resource_pool_action production.
	ExitAlter_resource_pool_action(c *Alter_resource_pool_actionContext)

	// ExitDrop_resource_pool_stmt is called when exiting the drop_resource_pool_stmt production.
	ExitDrop_resource_pool_stmt(c *Drop_resource_pool_stmtContext)

	// ExitCreate_resource_pool_classifier_stmt is called when exiting the create_resource_pool_classifier_stmt production.
	ExitCreate_resource_pool_classifier_stmt(c *Create_resource_pool_classifier_stmtContext)

	// ExitAlter_resource_pool_classifier_stmt is called when exiting the alter_resource_pool_classifier_stmt production.
	ExitAlter_resource_pool_classifier_stmt(c *Alter_resource_pool_classifier_stmtContext)

	// ExitAlter_resource_pool_classifier_action is called when exiting the alter_resource_pool_classifier_action production.
	ExitAlter_resource_pool_classifier_action(c *Alter_resource_pool_classifier_actionContext)

	// ExitDrop_resource_pool_classifier_stmt is called when exiting the drop_resource_pool_classifier_stmt production.
	ExitDrop_resource_pool_classifier_stmt(c *Drop_resource_pool_classifier_stmtContext)

	// ExitCreate_replication_stmt is called when exiting the create_replication_stmt production.
	ExitCreate_replication_stmt(c *Create_replication_stmtContext)

	// ExitReplication_target is called when exiting the replication_target production.
	ExitReplication_target(c *Replication_targetContext)

	// ExitReplication_settings is called when exiting the replication_settings production.
	ExitReplication_settings(c *Replication_settingsContext)

	// ExitReplication_settings_entry is called when exiting the replication_settings_entry production.
	ExitReplication_settings_entry(c *Replication_settings_entryContext)

	// ExitAlter_replication_stmt is called when exiting the alter_replication_stmt production.
	ExitAlter_replication_stmt(c *Alter_replication_stmtContext)

	// ExitAlter_replication_action is called when exiting the alter_replication_action production.
	ExitAlter_replication_action(c *Alter_replication_actionContext)

	// ExitAlter_replication_set_setting is called when exiting the alter_replication_set_setting production.
	ExitAlter_replication_set_setting(c *Alter_replication_set_settingContext)

	// ExitDrop_replication_stmt is called when exiting the drop_replication_stmt production.
	ExitDrop_replication_stmt(c *Drop_replication_stmtContext)

	// ExitAction_or_subquery_args is called when exiting the action_or_subquery_args production.
	ExitAction_or_subquery_args(c *Action_or_subquery_argsContext)

	// ExitDefine_action_or_subquery_stmt is called when exiting the define_action_or_subquery_stmt production.
	ExitDefine_action_or_subquery_stmt(c *Define_action_or_subquery_stmtContext)

	// ExitDefine_action_or_subquery_body is called when exiting the define_action_or_subquery_body production.
	ExitDefine_action_or_subquery_body(c *Define_action_or_subquery_bodyContext)

	// ExitIf_stmt is called when exiting the if_stmt production.
	ExitIf_stmt(c *If_stmtContext)

	// ExitFor_stmt is called when exiting the for_stmt production.
	ExitFor_stmt(c *For_stmtContext)

	// ExitTable_ref is called when exiting the table_ref production.
	ExitTable_ref(c *Table_refContext)

	// ExitTable_key is called when exiting the table_key production.
	ExitTable_key(c *Table_keyContext)

	// ExitTable_arg is called when exiting the table_arg production.
	ExitTable_arg(c *Table_argContext)

	// ExitTable_hints is called when exiting the table_hints production.
	ExitTable_hints(c *Table_hintsContext)

	// ExitTable_hint is called when exiting the table_hint production.
	ExitTable_hint(c *Table_hintContext)

	// ExitObject_ref is called when exiting the object_ref production.
	ExitObject_ref(c *Object_refContext)

	// ExitSimple_table_ref_core is called when exiting the simple_table_ref_core production.
	ExitSimple_table_ref_core(c *Simple_table_ref_coreContext)

	// ExitSimple_table_ref is called when exiting the simple_table_ref production.
	ExitSimple_table_ref(c *Simple_table_refContext)

	// ExitInto_simple_table_ref is called when exiting the into_simple_table_ref production.
	ExitInto_simple_table_ref(c *Into_simple_table_refContext)

	// ExitDelete_stmt is called when exiting the delete_stmt production.
	ExitDelete_stmt(c *Delete_stmtContext)

	// ExitUpdate_stmt is called when exiting the update_stmt production.
	ExitUpdate_stmt(c *Update_stmtContext)

	// ExitSet_clause_choice is called when exiting the set_clause_choice production.
	ExitSet_clause_choice(c *Set_clause_choiceContext)

	// ExitSet_clause_list is called when exiting the set_clause_list production.
	ExitSet_clause_list(c *Set_clause_listContext)

	// ExitSet_clause is called when exiting the set_clause production.
	ExitSet_clause(c *Set_clauseContext)

	// ExitSet_target is called when exiting the set_target production.
	ExitSet_target(c *Set_targetContext)

	// ExitMultiple_column_assignment is called when exiting the multiple_column_assignment production.
	ExitMultiple_column_assignment(c *Multiple_column_assignmentContext)

	// ExitSet_target_list is called when exiting the set_target_list production.
	ExitSet_target_list(c *Set_target_listContext)

	// ExitCreate_topic_stmt is called when exiting the create_topic_stmt production.
	ExitCreate_topic_stmt(c *Create_topic_stmtContext)

	// ExitCreate_topic_entries is called when exiting the create_topic_entries production.
	ExitCreate_topic_entries(c *Create_topic_entriesContext)

	// ExitCreate_topic_entry is called when exiting the create_topic_entry production.
	ExitCreate_topic_entry(c *Create_topic_entryContext)

	// ExitWith_topic_settings is called when exiting the with_topic_settings production.
	ExitWith_topic_settings(c *With_topic_settingsContext)

	// ExitAlter_topic_stmt is called when exiting the alter_topic_stmt production.
	ExitAlter_topic_stmt(c *Alter_topic_stmtContext)

	// ExitAlter_topic_action is called when exiting the alter_topic_action production.
	ExitAlter_topic_action(c *Alter_topic_actionContext)

	// ExitAlter_topic_add_consumer is called when exiting the alter_topic_add_consumer production.
	ExitAlter_topic_add_consumer(c *Alter_topic_add_consumerContext)

	// ExitTopic_create_consumer_entry is called when exiting the topic_create_consumer_entry production.
	ExitTopic_create_consumer_entry(c *Topic_create_consumer_entryContext)

	// ExitAlter_topic_alter_consumer is called when exiting the alter_topic_alter_consumer production.
	ExitAlter_topic_alter_consumer(c *Alter_topic_alter_consumerContext)

	// ExitAlter_topic_alter_consumer_entry is called when exiting the alter_topic_alter_consumer_entry production.
	ExitAlter_topic_alter_consumer_entry(c *Alter_topic_alter_consumer_entryContext)

	// ExitAlter_topic_drop_consumer is called when exiting the alter_topic_drop_consumer production.
	ExitAlter_topic_drop_consumer(c *Alter_topic_drop_consumerContext)

	// ExitTopic_alter_consumer_set is called when exiting the topic_alter_consumer_set production.
	ExitTopic_alter_consumer_set(c *Topic_alter_consumer_setContext)

	// ExitTopic_alter_consumer_reset is called when exiting the topic_alter_consumer_reset production.
	ExitTopic_alter_consumer_reset(c *Topic_alter_consumer_resetContext)

	// ExitAlter_topic_set_settings is called when exiting the alter_topic_set_settings production.
	ExitAlter_topic_set_settings(c *Alter_topic_set_settingsContext)

	// ExitAlter_topic_reset_settings is called when exiting the alter_topic_reset_settings production.
	ExitAlter_topic_reset_settings(c *Alter_topic_reset_settingsContext)

	// ExitDrop_topic_stmt is called when exiting the drop_topic_stmt production.
	ExitDrop_topic_stmt(c *Drop_topic_stmtContext)

	// ExitTopic_settings is called when exiting the topic_settings production.
	ExitTopic_settings(c *Topic_settingsContext)

	// ExitTopic_settings_entry is called when exiting the topic_settings_entry production.
	ExitTopic_settings_entry(c *Topic_settings_entryContext)

	// ExitTopic_setting_value is called when exiting the topic_setting_value production.
	ExitTopic_setting_value(c *Topic_setting_valueContext)

	// ExitTopic_consumer_with_settings is called when exiting the topic_consumer_with_settings production.
	ExitTopic_consumer_with_settings(c *Topic_consumer_with_settingsContext)

	// ExitTopic_consumer_settings is called when exiting the topic_consumer_settings production.
	ExitTopic_consumer_settings(c *Topic_consumer_settingsContext)

	// ExitTopic_consumer_settings_entry is called when exiting the topic_consumer_settings_entry production.
	ExitTopic_consumer_settings_entry(c *Topic_consumer_settings_entryContext)

	// ExitTopic_consumer_setting_value is called when exiting the topic_consumer_setting_value production.
	ExitTopic_consumer_setting_value(c *Topic_consumer_setting_valueContext)

	// ExitTopic_ref is called when exiting the topic_ref production.
	ExitTopic_ref(c *Topic_refContext)

	// ExitTopic_consumer_ref is called when exiting the topic_consumer_ref production.
	ExitTopic_consumer_ref(c *Topic_consumer_refContext)

	// ExitNull_treatment is called when exiting the null_treatment production.
	ExitNull_treatment(c *Null_treatmentContext)

	// ExitFilter_clause is called when exiting the filter_clause production.
	ExitFilter_clause(c *Filter_clauseContext)

	// ExitWindow_name_or_specification is called when exiting the window_name_or_specification production.
	ExitWindow_name_or_specification(c *Window_name_or_specificationContext)

	// ExitWindow_name is called when exiting the window_name production.
	ExitWindow_name(c *Window_nameContext)

	// ExitWindow_clause is called when exiting the window_clause production.
	ExitWindow_clause(c *Window_clauseContext)

	// ExitWindow_definition_list is called when exiting the window_definition_list production.
	ExitWindow_definition_list(c *Window_definition_listContext)

	// ExitWindow_definition is called when exiting the window_definition production.
	ExitWindow_definition(c *Window_definitionContext)

	// ExitNew_window_name is called when exiting the new_window_name production.
	ExitNew_window_name(c *New_window_nameContext)

	// ExitWindow_specification is called when exiting the window_specification production.
	ExitWindow_specification(c *Window_specificationContext)

	// ExitWindow_specification_details is called when exiting the window_specification_details production.
	ExitWindow_specification_details(c *Window_specification_detailsContext)

	// ExitExisting_window_name is called when exiting the existing_window_name production.
	ExitExisting_window_name(c *Existing_window_nameContext)

	// ExitWindow_partition_clause is called when exiting the window_partition_clause production.
	ExitWindow_partition_clause(c *Window_partition_clauseContext)

	// ExitWindow_order_clause is called when exiting the window_order_clause production.
	ExitWindow_order_clause(c *Window_order_clauseContext)

	// ExitWindow_frame_clause is called when exiting the window_frame_clause production.
	ExitWindow_frame_clause(c *Window_frame_clauseContext)

	// ExitWindow_frame_units is called when exiting the window_frame_units production.
	ExitWindow_frame_units(c *Window_frame_unitsContext)

	// ExitWindow_frame_extent is called when exiting the window_frame_extent production.
	ExitWindow_frame_extent(c *Window_frame_extentContext)

	// ExitWindow_frame_between is called when exiting the window_frame_between production.
	ExitWindow_frame_between(c *Window_frame_betweenContext)

	// ExitWindow_frame_bound is called when exiting the window_frame_bound production.
	ExitWindow_frame_bound(c *Window_frame_boundContext)

	// ExitWindow_frame_exclusion is called when exiting the window_frame_exclusion production.
	ExitWindow_frame_exclusion(c *Window_frame_exclusionContext)

	// ExitUse_stmt is called when exiting the use_stmt production.
	ExitUse_stmt(c *Use_stmtContext)

	// ExitSubselect_stmt is called when exiting the subselect_stmt production.
	ExitSubselect_stmt(c *Subselect_stmtContext)

	// ExitNamed_nodes_stmt is called when exiting the named_nodes_stmt production.
	ExitNamed_nodes_stmt(c *Named_nodes_stmtContext)

	// ExitCommit_stmt is called when exiting the commit_stmt production.
	ExitCommit_stmt(c *Commit_stmtContext)

	// ExitRollback_stmt is called when exiting the rollback_stmt production.
	ExitRollback_stmt(c *Rollback_stmtContext)

	// ExitAnalyze_table is called when exiting the analyze_table production.
	ExitAnalyze_table(c *Analyze_tableContext)

	// ExitAnalyze_table_list is called when exiting the analyze_table_list production.
	ExitAnalyze_table_list(c *Analyze_table_listContext)

	// ExitAnalyze_stmt is called when exiting the analyze_stmt production.
	ExitAnalyze_stmt(c *Analyze_stmtContext)

	// ExitAlter_sequence_stmt is called when exiting the alter_sequence_stmt production.
	ExitAlter_sequence_stmt(c *Alter_sequence_stmtContext)

	// ExitAlter_sequence_action is called when exiting the alter_sequence_action production.
	ExitAlter_sequence_action(c *Alter_sequence_actionContext)

	// ExitIdentifier is called when exiting the identifier production.
	ExitIdentifier(c *IdentifierContext)

	// ExitId is called when exiting the id production.
	ExitId(c *IdContext)

	// ExitId_schema is called when exiting the id_schema production.
	ExitId_schema(c *Id_schemaContext)

	// ExitId_expr is called when exiting the id_expr production.
	ExitId_expr(c *Id_exprContext)

	// ExitId_expr_in is called when exiting the id_expr_in production.
	ExitId_expr_in(c *Id_expr_inContext)

	// ExitId_window is called when exiting the id_window production.
	ExitId_window(c *Id_windowContext)

	// ExitId_table is called when exiting the id_table production.
	ExitId_table(c *Id_tableContext)

	// ExitId_without is called when exiting the id_without production.
	ExitId_without(c *Id_withoutContext)

	// ExitId_hint is called when exiting the id_hint production.
	ExitId_hint(c *Id_hintContext)

	// ExitId_as_compat is called when exiting the id_as_compat production.
	ExitId_as_compat(c *Id_as_compatContext)

	// ExitAn_id is called when exiting the an_id production.
	ExitAn_id(c *An_idContext)

	// ExitAn_id_or_type is called when exiting the an_id_or_type production.
	ExitAn_id_or_type(c *An_id_or_typeContext)

	// ExitAn_id_schema is called when exiting the an_id_schema production.
	ExitAn_id_schema(c *An_id_schemaContext)

	// ExitAn_id_expr is called when exiting the an_id_expr production.
	ExitAn_id_expr(c *An_id_exprContext)

	// ExitAn_id_expr_in is called when exiting the an_id_expr_in production.
	ExitAn_id_expr_in(c *An_id_expr_inContext)

	// ExitAn_id_window is called when exiting the an_id_window production.
	ExitAn_id_window(c *An_id_windowContext)

	// ExitAn_id_table is called when exiting the an_id_table production.
	ExitAn_id_table(c *An_id_tableContext)

	// ExitAn_id_without is called when exiting the an_id_without production.
	ExitAn_id_without(c *An_id_withoutContext)

	// ExitAn_id_hint is called when exiting the an_id_hint production.
	ExitAn_id_hint(c *An_id_hintContext)

	// ExitAn_id_pure is called when exiting the an_id_pure production.
	ExitAn_id_pure(c *An_id_pureContext)

	// ExitAn_id_as_compat is called when exiting the an_id_as_compat production.
	ExitAn_id_as_compat(c *An_id_as_compatContext)

	// ExitView_name is called when exiting the view_name production.
	ExitView_name(c *View_nameContext)

	// ExitOpt_id_prefix is called when exiting the opt_id_prefix production.
	ExitOpt_id_prefix(c *Opt_id_prefixContext)

	// ExitCluster_expr is called when exiting the cluster_expr production.
	ExitCluster_expr(c *Cluster_exprContext)

	// ExitId_or_type is called when exiting the id_or_type production.
	ExitId_or_type(c *Id_or_typeContext)

	// ExitOpt_id_prefix_or_type is called when exiting the opt_id_prefix_or_type production.
	ExitOpt_id_prefix_or_type(c *Opt_id_prefix_or_typeContext)

	// ExitId_or_at is called when exiting the id_or_at production.
	ExitId_or_at(c *Id_or_atContext)

	// ExitId_table_or_type is called when exiting the id_table_or_type production.
	ExitId_table_or_type(c *Id_table_or_typeContext)

	// ExitId_table_or_at is called when exiting the id_table_or_at production.
	ExitId_table_or_at(c *Id_table_or_atContext)

	// ExitKeyword is called when exiting the keyword production.
	ExitKeyword(c *KeywordContext)

	// ExitKeyword_expr_uncompat is called when exiting the keyword_expr_uncompat production.
	ExitKeyword_expr_uncompat(c *Keyword_expr_uncompatContext)

	// ExitKeyword_table_uncompat is called when exiting the keyword_table_uncompat production.
	ExitKeyword_table_uncompat(c *Keyword_table_uncompatContext)

	// ExitKeyword_select_uncompat is called when exiting the keyword_select_uncompat production.
	ExitKeyword_select_uncompat(c *Keyword_select_uncompatContext)

	// ExitKeyword_alter_uncompat is called when exiting the keyword_alter_uncompat production.
	ExitKeyword_alter_uncompat(c *Keyword_alter_uncompatContext)

	// ExitKeyword_in_uncompat is called when exiting the keyword_in_uncompat production.
	ExitKeyword_in_uncompat(c *Keyword_in_uncompatContext)

	// ExitKeyword_window_uncompat is called when exiting the keyword_window_uncompat production.
	ExitKeyword_window_uncompat(c *Keyword_window_uncompatContext)

	// ExitKeyword_hint_uncompat is called when exiting the keyword_hint_uncompat production.
	ExitKeyword_hint_uncompat(c *Keyword_hint_uncompatContext)

	// ExitKeyword_as_compat is called when exiting the keyword_as_compat production.
	ExitKeyword_as_compat(c *Keyword_as_compatContext)

	// ExitKeyword_compat is called when exiting the keyword_compat production.
	ExitKeyword_compat(c *Keyword_compatContext)

	// ExitType_id is called when exiting the type_id production.
	ExitType_id(c *Type_idContext)

	// ExitBool_value is called when exiting the bool_value production.
	ExitBool_value(c *Bool_valueContext)

	// ExitReal is called when exiting the real production.
	ExitReal(c *RealContext)

	// ExitInteger is called when exiting the integer production.
	ExitInteger(c *IntegerContext)
}
