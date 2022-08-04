/*-------------------------------------------------------------------------
 *
 * executor.h
 *	  support for the POSTGRES executor module
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/executor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "executor/execdesc.h"
#include "nodes/lockoptions.h"
#include "nodes/parsenodes.h"
#include "utils/memutils.h"
#ifdef SMARTSSD
#include "access/table.h" // to use table_open & table_close
#include "utils/rel.h" // RelationGetDescr
#include "utils/builtins.h" // TextDatumGetCString
#endif

/*
 * The "eflags" argument to ExecutorStart and the various ExecInitNode
 * routines is a bitwise OR of the following flag bits, which tell the
 * called plan node what to expect.  Note that the flags will get modified
 * as they are passed down the plan tree, since an upper node may require
 * functionality in its subnode not demanded of the plan as a whole
 * (example: MergeJoin requires mark/restore capability in its inner input),
 * or an upper node may shield its input from some functionality requirement
 * (example: Materialize shields its input from needing to do backward scan).
 *
 * EXPLAIN_ONLY indicates that the plan tree is being initialized just so
 * EXPLAIN can print it out; it will not be run.  Hence, no side-effects
 * of startup should occur.  However, error checks (such as permission checks)
 * should be performed.
 *
 * REWIND indicates that the plan node should try to efficiently support
 * rescans without parameter changes.  (Nodes must support ExecReScan calls
 * in any case, but if this flag was not given, they are at liberty to do it
 * through complete recalculation.  Note that a parameter change forces a
 * full recalculation in any case.)
 *
 * BACKWARD indicates that the plan node must respect the es_direction flag.
 * When this is not passed, the plan node will only be run forwards.
 *
 * MARK indicates that the plan node must support Mark/Restore calls.
 * When this is not passed, no Mark/Restore will occur.
 *
 * SKIP_TRIGGERS tells ExecutorStart/ExecutorFinish to skip calling
 * AfterTriggerBeginQuery/AfterTriggerEndQuery.  This does not necessarily
 * mean that the plan can't queue any AFTER triggers; just that the caller
 * is responsible for there being a trigger context for them to be queued in.
 */
#define EXEC_FLAG_EXPLAIN_ONLY	0x0001	/* EXPLAIN, no ANALYZE */
#define EXEC_FLAG_REWIND		0x0002	/* need efficient rescan */
#define EXEC_FLAG_BACKWARD		0x0004	/* need backward scan */
#define EXEC_FLAG_MARK			0x0008	/* need mark/restore */
#define EXEC_FLAG_SKIP_TRIGGERS 0x0010	/* skip AfterTrigger calls */
#define EXEC_FLAG_WITH_NO_DATA	0x0020	/* rel scannability doesn't matter */


/* Hook for plugins to get control in ExecutorStart() */
typedef void (*ExecutorStart_hook_type) (QueryDesc *queryDesc, int eflags);
extern PGDLLIMPORT ExecutorStart_hook_type ExecutorStart_hook;

/* Hook for plugins to get control in ExecutorRun() */
typedef void (*ExecutorRun_hook_type) (QueryDesc *queryDesc,
									   ScanDirection direction,
									   uint64 count,
									   bool execute_once);
extern PGDLLIMPORT ExecutorRun_hook_type ExecutorRun_hook;

/* Hook for plugins to get control in ExecutorFinish() */
typedef void (*ExecutorFinish_hook_type) (QueryDesc *queryDesc);
extern PGDLLIMPORT ExecutorFinish_hook_type ExecutorFinish_hook;

/* Hook for plugins to get control in ExecutorEnd() */
typedef void (*ExecutorEnd_hook_type) (QueryDesc *queryDesc);
extern PGDLLIMPORT ExecutorEnd_hook_type ExecutorEnd_hook;

/* Hook for plugins to get control in ExecCheckRTPerms() */
typedef bool (*ExecutorCheckPerms_hook_type) (List *, bool);
extern PGDLLIMPORT ExecutorCheckPerms_hook_type ExecutorCheckPerms_hook;


/*
 * prototypes from functions in execAmi.c
 */
struct Path;					/* avoid including pathnodes.h here */

extern void ExecReScan(PlanState *node);
extern void ExecMarkPos(PlanState *node);
extern void ExecRestrPos(PlanState *node);
extern bool ExecSupportsMarkRestore(struct Path *pathnode);
extern bool ExecSupportsBackwardScan(Plan *node);
extern bool ExecMaterializesOutput(NodeTag plantype);

/*
 * prototypes from functions in execCurrent.c
 */
extern bool execCurrentOf(CurrentOfExpr *cexpr,
						  ExprContext *econtext,
						  Oid table_oid,
						  ItemPointer current_tid);

/*
 * prototypes from functions in execGrouping.c
 */
extern ExprState *execTuplesMatchPrepare(TupleDesc desc,
										 int numCols,
										 const AttrNumber *keyColIdx,
										 const Oid *eqOperators,
										 const Oid *collations,
										 PlanState *parent);
extern void execTuplesHashPrepare(int numCols,
								  const Oid *eqOperators,
								  Oid **eqFuncOids,
								  FmgrInfo **hashFunctions);
extern TupleHashTable BuildTupleHashTable(PlanState *parent,
										  TupleDesc inputDesc,
										  int numCols, AttrNumber *keyColIdx,
										  const Oid *eqfuncoids,
										  FmgrInfo *hashfunctions,
										  Oid *collations,
										  long nbuckets, Size additionalsize,
										  MemoryContext tablecxt,
										  MemoryContext tempcxt, bool use_variable_hash_iv);
extern TupleHashTable BuildTupleHashTableExt(PlanState *parent,
											 TupleDesc inputDesc,
											 int numCols, AttrNumber *keyColIdx,
											 const Oid *eqfuncoids,
											 FmgrInfo *hashfunctions,
											 Oid *collations,
											 long nbuckets, Size additionalsize,
											 MemoryContext metacxt,
											 MemoryContext tablecxt,
											 MemoryContext tempcxt, bool use_variable_hash_iv);
extern TupleHashEntry LookupTupleHashEntry(TupleHashTable hashtable,
										   TupleTableSlot *slot,
										   bool *isnew);
extern TupleHashEntry FindTupleHashEntry(TupleHashTable hashtable,
										 TupleTableSlot *slot,
										 ExprState *eqcomp,
										 FmgrInfo *hashfunctions);
extern void ResetTupleHashTable(TupleHashTable hashtable);

/*
 * prototypes from functions in execJunk.c
 */
extern JunkFilter *ExecInitJunkFilter(List *targetList,
									  TupleTableSlot *slot);
extern JunkFilter *ExecInitJunkFilterConversion(List *targetList,
												TupleDesc cleanTupType,
												TupleTableSlot *slot);
extern AttrNumber ExecFindJunkAttribute(JunkFilter *junkfilter,
										const char *attrName);
extern AttrNumber ExecFindJunkAttributeInTlist(List *targetlist,
											   const char *attrName);
extern Datum ExecGetJunkAttribute(TupleTableSlot *slot, AttrNumber attno,
								  bool *isNull);
extern TupleTableSlot *ExecFilterJunk(JunkFilter *junkfilter,
									  TupleTableSlot *slot);


/*
 * prototypes from functions in execMain.c
 */
extern void ExecutorStart(QueryDesc *queryDesc, int eflags);
extern void standard_ExecutorStart(QueryDesc *queryDesc, int eflags);
extern void ExecutorRun(QueryDesc *queryDesc,
						ScanDirection direction, uint64 count, bool execute_once);
extern void standard_ExecutorRun(QueryDesc *queryDesc,
								 ScanDirection direction, uint64 count, bool execute_once);
extern void ExecutorFinish(QueryDesc *queryDesc);
extern void standard_ExecutorFinish(QueryDesc *queryDesc);
extern void ExecutorEnd(QueryDesc *queryDesc);
extern void standard_ExecutorEnd(QueryDesc *queryDesc);
extern void ExecutorRewind(QueryDesc *queryDesc);
extern bool ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation);
extern void CheckValidResultRel(ResultRelInfo *resultRelInfo, CmdType operation);
extern void InitResultRelInfo(ResultRelInfo *resultRelInfo,
							  Relation resultRelationDesc,
							  Index resultRelationIndex,
							  Relation partition_root,
							  int instrument_options);
extern ResultRelInfo *ExecGetTriggerResultRel(EState *estate, Oid relid);
extern void ExecCleanUpTriggerState(EState *estate);
extern void ExecConstraints(ResultRelInfo *resultRelInfo,
							TupleTableSlot *slot, EState *estate);
extern bool ExecPartitionCheck(ResultRelInfo *resultRelInfo,
							   TupleTableSlot *slot, EState *estate, bool emitError);
extern void ExecPartitionCheckEmitError(ResultRelInfo *resultRelInfo,
										TupleTableSlot *slot, EState *estate);
extern void ExecWithCheckOptions(WCOKind kind, ResultRelInfo *resultRelInfo,
								 TupleTableSlot *slot, EState *estate);
extern LockTupleMode ExecUpdateLockMode(EState *estate, ResultRelInfo *relinfo);
extern ExecRowMark *ExecFindRowMark(EState *estate, Index rti, bool missing_ok);
extern ExecAuxRowMark *ExecBuildAuxRowMark(ExecRowMark *erm, List *targetlist);
extern TupleTableSlot *EvalPlanQual(EPQState *epqstate, Relation relation,
									Index rti, TupleTableSlot *testslot);
extern void EvalPlanQualInit(EPQState *epqstate, EState *parentestate,
							 Plan *subplan, List *auxrowmarks, int epqParam);
extern void EvalPlanQualSetPlan(EPQState *epqstate,
								Plan *subplan, List *auxrowmarks);
extern TupleTableSlot *EvalPlanQualSlot(EPQState *epqstate,
										Relation relation, Index rti);

#define EvalPlanQualSetSlot(epqstate, slot)  ((epqstate)->origslot = (slot))
extern bool EvalPlanQualFetchRowMark(EPQState *epqstate, Index rti, TupleTableSlot *slot);
extern TupleTableSlot *EvalPlanQualNext(EPQState *epqstate);
extern void EvalPlanQualBegin(EPQState *epqstate);
extern void EvalPlanQualEnd(EPQState *epqstate);

/*
 * functions in execProcnode.c
 */
extern PlanState *ExecInitNode(Plan *node, EState *estate, int eflags);
extern void ExecSetExecProcNode(PlanState *node, ExecProcNodeMtd function);
extern Node *MultiExecProcNode(PlanState *node);
extern void ExecEndNode(PlanState *node);
extern bool ExecShutdownNode(PlanState *node);
extern void ExecSetTupleBound(int64 tuples_needed, PlanState *child_node);

#ifdef SMARTSSD
PlanState *g_ops;

typedef struct
{
	Oid opno;
	Oid relid;
	Oid attnum;
	Const *constval;
} s3d_filter;

typedef struct
{
	bool isjoin;
	PlanState *ps_outer;
	PlanState *ps_inner;
	Oid outer_relid;
	List *outer_attnum_list;
	Oid inner_relid;
	List *inner_attnum_list;
 	List *filter_list; /* List of s3d_filter */
} s3d_hj_clause_filter;

typedef struct
{
	List       *rtable;                     /* List of RangeTblEntry nodes */
	List       *rtable_names;       /* Parallel list of names for RTEs */
	List       *rtable_columns; /* Parallel list of deparse_columns structs */
	List       *ctes;                       /* List of CommonTableExpr nodes */
	/* Workspace for column alias assignment: */
	bool            unique_using;   /* Are we making USING names globally unique */
	List       *using_names;        /* List of assigned names for USING columns */
	/* Remaining fields are used only when deparsing a Plan tree: */
	PlanState  *planstate;          /* immediate parent of current expression */
	List       *ancestors;          /* ancestors of planstate */
	PlanState  *outer_planstate;    /* outer subplan state, or NULL if none */
	PlanState  *inner_planstate;    /* inner subplan state, or NULL if none */
	List       *outer_tlist;        /* referent for OUTER_VAR Vars */
	List       *inner_tlist;        /* referent for INNER_VAR Vars */
	List       *index_tlist;        /* referent for INDEX_VAR Vars */
} deparse_namespace;

typedef struct
{
	/*
	 * colnames is an array containing column aliases to use for columns that
	 * existed when the query was parsed.  Dropped columns have NULL entries.
	 * This array can be directly indexed by varattno to get a Var's name.
	 *
	 * Non-NULL entries are guaranteed unique within the RTE, *except* when
	 * this is for an unnamed JOIN RTE.  In that case we merely copy up names
	 * from the two input RTEs.
	 *
	 * During the recursive descent in set_using_names(), forcible assignment
	 * of a child RTE's column name is represented by pre-setting that element
	 * of the child's colnames array.  So at that stage, NULL entries in this
	 * array just mean that no name has been preassigned, not necessarily that
	 * the column is dropped.
	 */
	int			num_cols;		/* length of colnames[] array */
	char	  **colnames;		/* array of C strings and NULLs */

	/*
	 * new_colnames is an array containing column aliases to use for columns
	 * that would exist if the query was re-parsed against the current
	 * definitions of its base tables.  This is what to print as the column
	 * alias list for the RTE.  This array does not include dropped columns,
	 * but it will include columns added since original parsing.  Indexes in
	 * it therefore have little to do with current varattno values.  As above,
	 * entries are unique unless this is for an unnamed JOIN RTE.  (In such an
	 * RTE, we never actually print this array, but we must compute it anyway
	 * for possible use in computing column names of upper joins.) The
	 * parallel array is_new_col marks which of these columns are new since
	 * original parsing.  Entries with is_new_col false must match the
	 * non-NULL colnames entries one-for-one.
	 */
	int			num_new_cols;	/* length of new_colnames[] array */
	char	  **new_colnames;	/* array of C strings */
	bool	   *is_new_col;		/* array of bool flags */

	/* This flag tells whether we should actually print a column alias list */
	bool		printaliases;

	/* This list has all names used as USING names in joins above this RTE */
	List	   *parentUsing;	/* names assigned to parent merged columns */

	/*
	 * If this struct is for a JOIN RTE, we fill these fields during the
	 * set_using_names() pass to describe its relationship to its child RTEs.
	 *
	 * leftattnos and rightattnos are arrays with one entry per existing
	 * output column of the join (hence, indexable by join varattno).  For a
	 * simple reference to a column of the left child, leftattnos[i] is the
	 * child RTE's attno and rightattnos[i] is zero; and conversely for a
	 * column of the right child.  But for merged columns produced by JOIN
	 * USING/NATURAL JOIN, both leftattnos[i] and rightattnos[i] are nonzero.
	 * Also, if the column has been dropped, both are zero.
	 *
	 * If it's a JOIN USING, usingNames holds the alias names selected for the
	 * merged columns (these might be different from the original USING list,
	 * if we had to modify names to achieve uniqueness).
	 */
	int			leftrti;		/* rangetable index of left child */
	int			rightrti;		/* rangetable index of right child */
	int		   *leftattnos;		/* left-child varattnos of join cols, or 0 */
	int		   *rightattnos;	/* right-child varattnos of join cols, or 0 */
	List	   *usingNames;		/* names assigned to merged columns */
} deparse_columns;

/* This macro is analogous to rt_fetch(), but for deparse_columns structs */
#define deparse_columns_fetch(rangetable_index, dpns) \
	((deparse_columns *) list_nth((dpns)->rtable_columns, (rangetable_index)-1))

extern Expr *make_ands_explicit(List *andclauses);
extern TargetEntry *get_tle_by_resno(List *tlist, AttrNumber resno);
extern char *get_rel_name(Oid relid); // for debug only
extern char *get_rte_attribute_name(RangeTblEntry *rte, AttrNumber attnum);

#ifndef FRONTEND
static inline List *
GetS3DNode(PlanState *planstate, s3d_hj_clause_filter *cf);

static inline void
GetSubPlans(List *plans, s3d_hj_clause_filter *cf);

static inline bool
EvalFilter(s3d_filter *filter);

static inline void
get_qual_s3d(List *qual, PlanState *planstate, s3d_hj_clause_filter *cf);

static inline List *
set_deparse_context_planstate_s3d(List *dpcontext, Node *planstate);

static inline void 
set_deparse_planstate_s3d(deparse_namespace *dpns, PlanState *ps);

static inline List *
get_func_expr_s3d(FuncExpr *expr, deparse_namespace *dpns, s3d_hj_clause_filter *cf);

static inline void
get_oper_expr_s3d(OpExpr *expr, deparse_namespace *dpns, s3d_hj_clause_filter *cf);

static inline void
get_bool_expr_s3d(BoolExpr *expr, deparse_namespace *dpns, s3d_hj_clause_filter *cf);

static inline List *
get_rule_expr_s3d(Node *node, deparse_namespace *dpns, s3d_hj_clause_filter *cf);

static inline List *
get_variable_s3d(Var *var, deparse_namespace *dpns);

static inline List *
resolve_special_varno_s3d(Var *var, deparse_namespace *dpns);

static inline void 
set_deparse_planstate_s3d(deparse_namespace *dpns, PlanState *ps)
{
	dpns->planstate = ps;

	/*
	 * We special-case Append and MergeAppend to pretend that the first child
	 * plan is the OUTER referent; we have to interpret OUTER Vars in their
	 * tlists according to one of the children, and the first one is the most
	 * natural choice.  Likewise special-case ModifyTable to pretend that the
	 * first child plan is the OUTER referent; this is to support RETURNING
	 * lists containing references to non-target relations.
	 */
	if (IsA(ps, AppendState))
		dpns->outer_planstate = ((AppendState *) ps)->appendplans[0];
	else if (IsA(ps, MergeAppendState))
		dpns->outer_planstate = ((MergeAppendState *) ps)->mergeplans[0];
	else if (IsA(ps, ModifyTableState))
		dpns->outer_planstate = ((ModifyTableState *) ps)->mt_plans[0];
	else
		dpns->outer_planstate = outerPlanState(ps);

	if (dpns->outer_planstate)
		dpns->outer_tlist = dpns->outer_planstate->plan->targetlist;
	else
		dpns->outer_tlist = NIL;
#ifdef SMARTSSD
	if (dpns->outer_planstate && nodeTag(dpns->outer_planstate->plan) == T_SeqScan)
		g_ops = dpns->outer_planstate;
#endif
	/*
	 * For a SubqueryScan, pretend the subplan is INNER referent.  (We don't
	 * use OUTER because that could someday conflict with the normal meaning.)
	 * Likewise, for a CteScan, pretend the subquery's plan is INNER referent.
	 * For ON CONFLICT .. UPDATE we just need the inner tlist to point to the
	 * excluded expression's tlist. (Similar to the SubqueryScan we don't want
	 * to reuse OUTER, it's used for RETURNING in some modify table cases,
	 * although not INSERT .. CONFLICT).
	 */
	if (IsA(ps, SubqueryScanState))
		dpns->inner_planstate = ((SubqueryScanState *) ps)->subplan;
	else if (IsA(ps, CteScanState))
		dpns->inner_planstate = ((CteScanState *) ps)->cteplanstate;
	else if (IsA(ps, ModifyTableState))
		dpns->inner_planstate = ps;
	else
		dpns->inner_planstate = innerPlanState(ps);

	if (IsA(ps, ModifyTableState))
		dpns->inner_tlist = ((ModifyTableState *) ps)->mt_excludedtlist;
	else if (dpns->inner_planstate)
		dpns->inner_tlist = dpns->inner_planstate->plan->targetlist;
	else
		dpns->inner_tlist = NIL;

	/* Set up referent for INDEX_VAR Vars, if needed */
	if (IsA(ps->plan, IndexOnlyScan))
		dpns->index_tlist = ((IndexOnlyScan *) ps->plan)->indextlist;
	else if (IsA(ps->plan, ForeignScan))
		dpns->index_tlist = ((ForeignScan *) ps->plan)->fdw_scan_tlist;
	else if (IsA(ps->plan, CustomScan))
		dpns->index_tlist = ((CustomScan *) ps->plan)->custom_scan_tlist;
	else
		dpns->index_tlist = NIL;
}

static inline List *
set_deparse_context_planstate_s3d(List *dpcontext, Node *planstate)
{
	deparse_namespace *dpns;
 
 	/* Should always have one-entry namespace list for Plan deparsing */
	Assert(list_length(dpcontext) == 1);
	dpns = (deparse_namespace *) linitial(dpcontext);
 
 	/* Set our attention on the specific plan node passed in */
	set_deparse_planstate_s3d(dpns, (PlanState *) planstate);

	return dpcontext;
}

static inline void 
push_child_plan_s3d(deparse_namespace *dpns, PlanState *ps, deparse_namespace *save_dpns)
{
	*save_dpns = *dpns;
	set_deparse_planstate_s3d(dpns, ps);
}

static inline void 
pop_child_plan_s3d(deparse_namespace *dpns, deparse_namespace *save_dpns)
{
	*dpns = *save_dpns;
}

static inline List *
get_func_expr_s3d(FuncExpr *expr, deparse_namespace *dpns, s3d_hj_clause_filter *cf)
{
	List *ret = NIL;
	
	/*
	 * If the function call came from an implicit coercion, then just show the
	 * first argument --- unless caller wants to see implicit coercions.
	 */
	if (expr->funcformat == COERCE_IMPLICIT_CAST)
	{
		ret = get_rule_expr_s3d((Node *) linitial(expr->args), dpns, cf);
		return ret;
	}

	return ret;
}

static inline void
get_oper_expr_s3d(OpExpr *expr, deparse_namespace *dpns, s3d_hj_clause_filter *cf)
{
	s3d_filter *filter;
	Oid opno = expr->opno;
	List *args = expr->args, *lo, *li;
#ifdef SMARTSSD_DEBUG
	fprintf(stderr, "[SMARTSSD] opno:%d, %s %d %s\n", opno, __FILE__, __LINE__, __func__);
#endif
	if (list_length(args) == 2)
	{
		/* binary operator */
		Node *arg1 = (Node *) linitial(args);
		Node *arg2 = (Node *) lsecond(args);
		
		if (cf->isjoin)
		{
			/* allow SeqScan only, because results from hash join 
			 * are requested when SeqNext() is called */
			lo = get_rule_expr_s3d(arg1, dpns, cf);
			if (!g_ops || nodeTag(g_ops->plan) != T_SeqScan)
				return;
			if (cf->ps_outer == NULL)
				cf->ps_outer = g_ops;
			
			g_ops = NULL;
			li = get_rule_expr_s3d(arg2, dpns, cf);
			if (!g_ops || nodeTag(g_ops->plan) != T_SeqScan)
				return;
			if (cf->ps_inner == NULL)
				cf->ps_inner = g_ops;
			
			cf->outer_relid = linitial_oid(lo);
			cf->outer_attnum_list = lappend_oid(cf->outer_attnum_list, lsecond_oid(lo));

			cf->inner_relid = linitial_oid(li);
			cf->inner_attnum_list = lappend_oid(cf->inner_attnum_list, lsecond_oid(li));
		}
		else		
		{
			/* An operation expression that does not have an immediate value 
			 * (i.e. the value of a.id = another column value) cannot have 
			 * a "constval" field in the filter for an NDP operation. */
			lo = get_rule_expr_s3d(arg1, dpns, cf); 
			
			if (nodeTag(arg2) == T_Const)
			{
				filter = (s3d_filter *)palloc0(sizeof(s3d_filter));			
				filter->opno = opno;
				filter->relid = linitial_oid(lo);
				filter->attnum = lsecond_oid(lo);
				filter->constval = NULL; 
				cf->filter_list = lappend(cf->filter_list, filter);
				
				get_rule_expr_s3d(arg2, dpns, cf); // const

				if (!EvalFilter(filter))
				{
					/* Remove the inappropriate filter from the list */
					cf->filter_list = list_delete_ptr(cf->filter_list, filter);
					pfree(filter);
				}
			}
		}
	}
#if 0	
	else
	{
		// unary operator --- but which side? 
		Node	   *arg = (Node *) linitial(args);
		HeapTuple	tp;
		Form_pg_operator optup;

		tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for operator %u", opno);
		optup = (Form_pg_operator) GETSTRUCT(tp);
		switch (optup->oprkind)
		{
		case 'l':
			get_rule_expr_paren(arg, context, true, (Node *) expr);
			break;
		case 'r':
			get_rule_expr_paren(arg, context, true, (Node *) expr);
			break;
		default:
			elog(ERROR, "bogus oprkind: %d", optup->oprkind);
		}
		ReleaseSysCache(tp);
	}
#endif	
}

static inline void
get_bool_expr_s3d(BoolExpr *expr, deparse_namespace *dpns, s3d_hj_clause_filter *cf)
{
	//BoolExpr   *expr = (BoolExpr *) node;
	Node	   *first_arg = linitial(expr->args);
	ListCell   *arg = lnext(list_head(expr->args));

	switch (expr->boolop)
	{
	case AND_EXPR:
		get_rule_expr_s3d(first_arg, dpns, cf);
		while (arg)
		{
			get_rule_expr_s3d((Node *) lfirst(arg), dpns, cf);
			arg = lnext(arg);
		}
		break;

	case OR_EXPR:
		get_rule_expr_s3d(first_arg, dpns, cf);
		while (arg)
		{
			get_rule_expr_s3d((Node *) lfirst(arg), dpns, cf);
			arg = lnext(arg);
		}
		break;

	case NOT_EXPR:
		get_rule_expr_s3d(first_arg, dpns, cf);
		break;

	default:
		elog(ERROR, "unrecognized boolop: %d", (int) expr->boolop);
	}
}

static inline List *
get_rule_expr_s3d(Node *node, deparse_namespace *dpns, s3d_hj_clause_filter *cf)
{
	s3d_filter *filter;
	List *ret = NIL;

	if (node == NULL)
		return ret;

	/* Guard against excessively long or deeply-nested queries */
	//CHECK_FOR_INTERRUPTS();
	//check_stack_depth();

	/*
	 * Each level of get_rule_expr must emit an indivisible term
	 * (parenthesized if necessary) to ensure result is reparsed into the same
	 * expression tree.  The only exception is that when the input is a List,
	 * we emit the component items comma-separated with no surrounding
	 * decoration; this is convenient for most callers.
	 */
	switch (nodeTag(node))
	{
	case T_Var:
		ret = get_variable_s3d((Var *) node, dpns);
		break;

	case T_Const:
		if (list_length(cf->filter_list))
		{
			filter = (s3d_filter *) llast(cf->filter_list);
			if (filter->constval == NULL)
			{
				filter->constval = (Const *) node;
#ifdef SMARTSSD_DEBUG
				fprintf(stderr, "[SMARTSSD] consttype:%d, len:%d, %s %d %s\n", 
					((Const *) node)->consttype, ((Const *) node)->constlen,
					__FILE__, __LINE__, __func__);
#endif
			}
		}			
		break;

	case T_FuncExpr:
		ret = get_func_expr_s3d((FuncExpr *) node, dpns, cf);
		break;

	case T_OpExpr:
		get_oper_expr_s3d((OpExpr *) node, dpns, cf);
		break;
	
	case T_BoolExpr:
		get_bool_expr_s3d((BoolExpr *) node, dpns, cf);
		break;

	default:
#ifdef SMARTSSD_DEBUG
		fprintf(stderr, "[SMARTSSD] ignoring node type:%d\n", (int) nodeTag(node));
#endif //SMARTSSD_DEBUG
		break;
	}

	return ret;
}

static inline List *
get_variable_s3d(Var *var, deparse_namespace *dpns)
{
	RangeTblEntry *rte;
	deparse_columns *colinfo;
	AttrNumber attnum;
	List *l = NIL;
	char *attname;
#ifdef SMARTSSD_DEBUG
	char *refname;
#endif	
	if (var->varno >= 1 && var->varno <= list_length(dpns->rtable)) {
		rte = (RangeTblEntry *)list_nth(dpns->rtable, var->varno - 1);
#ifdef SMARTSSD_DEBUG
		refname = (char *)list_nth(dpns->rtable_names, var->varno - 1);
#endif	
		colinfo = deparse_columns_fetch(var->varno, dpns);
		attnum = var->varattno;
	} else {
		l = resolve_special_varno_s3d(var, dpns);
		return l;
	}

	if (attnum == InvalidAttrNumber) {
		attname = NULL;
	} else if (attnum > 0) {
		if (attnum > colinfo->num_cols) 
			elog(ERROR, "invalid attnum %d for relation \"%s\"", 
					attnum, rte->eref->aliasname);
		attname = colinfo->colnames[attnum - 1];		
		if (attname == NULL) /* dropped column? */ 
			elog(ERROR, "invalid attnum %d for relation \"%s\"", 
					attnum, rte->eref->aliasname);
	}
	else // System column
		//attname = get_rte_attribute_name(rte, attnum);
		;

	l = lappend_oid(l, rte->relid);		
	l = lappend_oid(l, attnum);		
#ifdef SMARTSSD_DEBUG
	fprintf(stderr, "[SMARTSSD] ref:%d(%s), attnum:%d(%s), %s %d %s\n", 
		rte->relid, refname, attnum, attname, __FILE__, __LINE__, __func__);
#endif
	return l;
}

static inline List *
resolve_special_varno_s3d(Var *var, deparse_namespace *dpns)
{
	TargetEntry *tle;
	deparse_namespace save_dpns;
	List *l = NIL;

	if (var->varno == OUTER_VAR && dpns->outer_tlist)
	{
		tle = get_tle_by_resno(dpns->outer_tlist, var->varattno);
		if (!tle) 
			elog(ERROR, "bogus varattno for OUTER_VAR var: %d", var->varno);

		push_child_plan_s3d(dpns, dpns->outer_planstate, &save_dpns);
		l = resolve_special_varno_s3d((Var *)tle->expr, dpns);
		pop_child_plan_s3d(dpns, &save_dpns);

		return l;
	}
	else if (var->varno == INNER_VAR && dpns->inner_tlist)
	{
		tle = get_tle_by_resno(dpns->inner_tlist, var->varattno);
		if (!tle) 
			elog(ERROR, "bogus varattno for INNER_VAR var: %d", var->varno);

		push_child_plan_s3d(dpns, dpns->inner_planstate, &save_dpns);
		l = resolve_special_varno_s3d((Var *)tle->expr, dpns);	
		pop_child_plan_s3d(dpns, &save_dpns);

		return l;	
	}
	else if (var->varno == INDEX_VAR && dpns->index_tlist)
	{
		return l;
	}
	else if (var->varno < 1 || var->varno > list_length(dpns->rtable)) 
		elog(ERROR, "bogus varno: %d", var->varno);

	return get_variable_s3d(var, dpns);
}

static inline List *
GetS3DNode(PlanState *planstate, s3d_hj_clause_filter *cf)
{
	List *ret = NIL;
	Plan *plan = planstate->plan;

	/* quals, sort keys, etc */
	switch (nodeTag(plan))
	{
	/* fall through to print additional fields the same as SeqScan */
	/* FALLTHROUGH */
	case T_SeqScan:
	case T_ValuesScan:
	case T_CteScan:
	case T_NamedTuplestoreScan:
	case T_WorkTableScan:
	case T_SubqueryScan:
#ifdef SMARTSSD_DEBUG	
		fprintf(stderr, "[SMARTSSD] Scan_begin:%s %d %s\n", __FILE__, __LINE__, __func__);
#endif		
		get_qual_s3d(plan->qual, planstate, cf);
#ifdef SMARTSSD_DEBUG	
		fprintf(stderr, "[SMARTSSD] Scan_end:%s %d %s\n", __FILE__, __LINE__, __func__);
#endif		
		break;

	case T_HashJoin:
		// When start visiting subjoin node, we should stop and return info on parent HJ node.
		if (cf->ps_inner != NULL && cf->ps_outer != NULL) 
		{
			return ret;
		}

		// Hash Cond
#ifdef SMARTSSD_DEBUG	
		fprintf(stderr, "[SMARTSSD] HJ_begin:%s %d %s\n", __FILE__, __LINE__, __func__);
#endif		
		cf->isjoin = true;
		get_qual_s3d(((HashJoin *) plan)->hashclauses, planstate, cf);
#ifdef SMARTSSD_DEBUG	
		fprintf(stderr, "[SMARTSSD] HJ_end:%s %d %s\n", __FILE__, __LINE__, __func__);
#endif
		// Join Filter, we ignore it!
		// cf->isjoin = true;
		// get_qual_s3d(((HashJoin *) plan)->join.joinqual, planstate, cf);

		// Filter
#ifdef SMARTSSD_DEBUG  
		fprintf(stderr, "[SMARTSSD] Filter_begin:%s %d %s\n", __FILE__, __LINE__, __func__);
#endif   
		cf->isjoin = false;
		get_qual_s3d(plan->qual, planstate, cf);
#ifdef SMARTSSD_DEBUG  
		fprintf(stderr, "[SMARTSSD] Filter_end:%s %d %s\n", __FILE__, __LINE__, __func__);
#endif   
		break;		

	case T_Hash:
		//show_hash_info_s3d(castNode(HashState, planstate));
		break;

	default:
		break;
	}		

	/*
	 * If partition pruning was done during executor initialization, the
	 * number of child plans we'll display below will be less than the number
	 * of subplans that was specified in the plan.  To make this a bit less
	 * mysterious, emit an indication that this happened.  Note that this
	 * field is emitted now because we want it to be a property of the parent
	 * node; it *cannot* be emitted within the Plans sub-node we'll open next.
	 */
#if 0
	switch (nodeTag(plan))
	{
	case T_Append:
		ExplainMissingMembers(((AppendState *) planstate)->as_nplans, 
			list_length(((Append *) plan)->appendplans));
			break;
	case T_MergeAppend:
		ExplainMissingMembers(((MergeAppendState *) planstate)->ms_nplans,
			list_length(((MergeAppend *) plan)->mergeplans));
		break;
	default:
		break;
	}
#endif
	/* initPlan-s */
	if (planstate->initPlan)
		GetSubPlans(planstate->initPlan, cf);

	/* lefttree */
	if (outerPlanState(planstate))
	{
		GetS3DNode(outerPlanState(planstate), cf);
	}

	/* righttree */
	if (innerPlanState(planstate))
	{
		GetS3DNode(innerPlanState(planstate), cf);
	}

	/* special child plans */
#if 0	
	switch (nodeTag(plan))
	{
	case T_ModifyTable:
		ExplainMemberNodes(((ModifyTableState *) planstate)->mt_plans,
			((ModifyTableState *) planstate)->mt_nplans);
		break;
	case T_Append:
		ExplainMemberNodes(((AppendState *) planstate)->appendplans,
			((AppendState *) planstate)->as_nplans);
		break;
	case T_MergeAppend:
		ExplainMemberNodes(((MergeAppendState *) planstate)->mergeplans,
			((MergeAppendState *) planstate)->ms_nplans);
		break;
	case T_BitmapAnd:
		ExplainMemberNodes(((BitmapAndState *) planstate)->bitmapplans,
			((BitmapAndState *) planstate)->nplans);
		break;
	case T_BitmapOr:
		ExplainMemberNodes(((BitmapOrState *) planstate)->bitmapplans,
			((BitmapOrState *) planstate)->nplans);
		break;
	case T_SubqueryScan:
		GetS3DNode(((SubqueryScanState *) planstate)->subplan, cf);
		break;
	case T_CustomScan:
		ExplainCustomChildren((CustomScanState *) planstate);
		break;
	default:
		break;
	}
#endif
	/* subPlan-s */
	if (planstate->subPlan)
		GetSubPlans(planstate->subPlan, cf);

	return ret;
}

/*
 * Get a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlanStates.
 */
static inline void
GetSubPlans(List *plans, s3d_hj_clause_filter *cf)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);
		//SubPlan    *sp = sps->subplan;

		/*
		 * There can be multiple SubPlan nodes referencing the same physical
		 * subplan (same plan_id, which is its index in PlannedStmt.subplans).
		 * We should print a subplan only once, so track which ones we already
		 * printed.  This state must be global across the plan tree, since the
		 * duplicate nodes could be in different plan nodes, eg both a bitmap
		 * indexscan's indexqual and its parent heapscan's recheck qual.  (We
		 * do not worry too much about which plan node we show the subplan as
		 * attached to in such cases.)
		 */
#if 0		 
		if (bms_is_member(sp->plan_id, es->printed_subplans))
			continue;
		es->printed_subplans = bms_add_member(es->printed_subplans, sp->plan_id);
#endif
		GetS3DNode(sps->planstate, cf);
	}
}

static inline void
get_qual_s3d(List *qual, PlanState *planstate, s3d_hj_clause_filter *cf)
{
	Node *node;
	List *context;
	deparse_namespace *dpns;
	
	// Convert AND list to explicit AND 
	node = (Node *)make_ands_explicit(qual);

	// Set up deparsing context 
	context = set_deparse_context_planstate_s3d(planstate->state->es_deparse_cxt, (Node *)planstate);

	dpns = (deparse_namespace *) linitial(context);

	// Deparse the expression 
	get_rule_expr_s3d(node, dpns, cf);
}

static inline bool
EvalFilter(s3d_filter *filter)
{
	Relation relation;
	TupleDesc tupleDesc;
	AttrNumber parent_attno;
	char *patt;
	bool allowed = true;

	relation = table_open(filter->relid, NoLock);
	tupleDesc = RelationGetDescr(relation);

	for (parent_attno = 1; parent_attno <= filter->attnum; parent_attno++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDesc, parent_attno - 1);

		// if varchar attribute appears
		if (attribute->attlen == -1)
		{
			if (parent_attno < filter->attnum)
			{
#ifdef SMARTSSD_DEBUG		
				fprintf(stderr, "[SMARTSSD] Varchar(%d) appears before filter(%d), "
					"%s %d %s\n", parent_attno, filter->attnum,
					__FILE__, __LINE__, __func__);
#endif			
				allowed = false;
				break;
			}	
			else // parent_attno == filter->attnum
			{
				// we allow exact/prefix match only
				if (!(filter->opno == 1054 /* = */ ||
					filter->opno == 1211 /* like */)) 
					allowed = false;

				if (filter->opno == 1211 /* like */)
				{
					patt = TextDatumGetCString(filter->constval->constvalue);
					/* % and _ are wildcard characters in LIKE */
					if (patt[0] == '%' || patt[0] == '_')
						allowed = false;
					pfree(patt);
				}	
			}
		}
	}

	table_close(relation, NoLock);
	return allowed;
}

static inline List *
GetOffloadTargets(PlanState *node)
{
	ListCell *lj;
#ifdef SMARTSSD_DEBUG
	ListCell *la;
#endif //SMARTSSD_DEBUG
	List *ret = NIL;
	s3d_hj_clause_filter *cf;
	bool isleaf = true;

	foreach (lj, node->state->es_join_tree)
	{
		g_ops = NULL;

		cf = (s3d_hj_clause_filter *)palloc0(sizeof(s3d_hj_clause_filter)); 
		cf->ps_outer = NULL;
		cf->ps_inner = NULL;
		cf->filter_list = NIL;

		GetS3DNode((PlanState *)lfirst(lj), cf);

		// When either of nodes is not sequentially scanned
		if ((!cf->ps_outer || !cf->ps_inner))
		{
			pfree(cf);
			continue;
		}

		if (isleaf)
		{
			// If leaf HJ node has no appropriate filters
			/*if (!list_length(cf->filter_list))
			{
				pfree(cf);
				return ret;
			}*/
 
			isleaf = false;
		}
#ifdef SMARTSSD_DEBUG
		fprintf(stderr, "[SMARTSSD] Join:%d = %d, "
			"outer_att_len:%d, inner_att_len:%d, filter_len:%d\n",
			cf->outer_relid, 
			cf->inner_relid, 
			list_length(cf->outer_attnum_list), 
			list_length(cf->inner_attnum_list), 
			list_length(cf->filter_list));
		
		fprintf(stderr, "[SMARTSSD] outer_attnum: ");
		foreach(la, cf->outer_attnum_list)
		{
			Oid attnum = lfirst_oid(la);
			fprintf(stderr, "%d ", attnum);
		}	
		
		fprintf(stderr, "\n[SMARTSSD] inner_attnum: ");
		foreach(la, cf->inner_attnum_list)
		{
			Oid attnum = lfirst_oid(la);
			fprintf(stderr, "%d ", attnum);
		}	
		fprintf(stderr, "\n");
#endif
		((SeqScanState *)(cf->ps_outer))->offload_to_dev = true;
		((SeqScanState *)(cf->ps_inner))->offload_to_dev = true;
#ifdef SMARTSSD_DEBUG
		fprintf(stderr, "[SMARTSSD] relid:%d, outer:%d, offload:%d, "
			"relid:%d, inner::%d, offload:%d, %s %d %s\n",
			cf->outer_relid,
			((SeqScanState *)(cf->ps_outer))->ss.ss_currentRelation->rd_node.relNode,
			((SeqScanState *)(cf->ps_outer))->offload_to_dev,
			cf->inner_relid,
			((SeqScanState *)(cf->ps_inner))->ss.ss_currentRelation->rd_node.relNode,
			((SeqScanState *)(cf->ps_inner))->offload_to_dev,
		__FILE__, __LINE__, __func__);
#endif
		ret = lappend(ret, cf);
	}

	return ret;
}
#endif // #ifndef FRONTEND
#endif // #ifdef SMARTSSD

#include <time.h>
struct QProfile {
	char name[64];
	ExecProcNodeMtd fn;
	unsigned long count;
	unsigned long time;
};

extern struct QProfile profiles[128];
extern int QProfile_target_pid;
extern int MyProcPid;

/* ----------------------------------------------------------------
 *		ExecProcNode
 *
 *		Execute the given node to return a(nother) tuple.
 * ----------------------------------------------------------------
 */

#ifndef FRONTEND
static inline TupleTableSlot *
ExecProcNode(PlanState *node)
{
	if (node->chgParam != NULL) /* something changed? */
		ExecReScan(node);		/* let ReScan handle this */

	/*if (QProfile_target_pid == MyProcPid) {
		type = nodeTag(node->plan);
		profile = &profiles[type];

		clock_gettime(CLOCK_MONOTONIC, &start);
		slot = node->ExecProcNode(node);
		clock_gettime(CLOCK_MONOTONIC, &end);

		if (end.tv_nsec > start.tv_nsec) {
			profile->fn = node->ExecProcNodeReal;
			profile->count++;
			profile->time += (end.tv_sec - start.tv_sec) * 1000000000
				+ (end.tv_nsec - start.tv_nsec);
		} else if (end.tv_sec >= start.tv_sec) {
			profile->fn = node->ExecProcNodeReal;
			profile->count++;
			profile->time += (end.tv_sec - start.tv_sec - 1) * 1000000000
				+ (1000000000 + end.tv_nsec - start.tv_nsec);
		}
		return slot;
	} else {*/
		return node->ExecProcNode(node);
	//}
}
#endif

/*
 * prototypes from functions in execExpr.c
 */
extern ExprState *ExecInitExpr(Expr *node, PlanState *parent);
extern ExprState *ExecInitExprWithParams(Expr *node, ParamListInfo ext_params);
extern ExprState *ExecInitQual(List *qual, PlanState *parent);
extern ExprState *ExecInitCheck(List *qual, PlanState *parent);
extern List *ExecInitExprList(List *nodes, PlanState *parent);
extern ExprState *ExecBuildAggTrans(AggState *aggstate, struct AggStatePerPhaseData *phase,
									bool doSort, bool doHash);
extern ExprState *ExecBuildGroupingEqual(TupleDesc ldesc, TupleDesc rdesc,
										 const TupleTableSlotOps *lops, const TupleTableSlotOps *rops,
										 int numCols,
										 const AttrNumber *keyColIdx,
										 const Oid *eqfunctions,
										 const Oid *collations,
										 PlanState *parent);
extern ProjectionInfo *ExecBuildProjectionInfo(List *targetList,
											   ExprContext *econtext,
											   TupleTableSlot *slot,
											   PlanState *parent,
											   TupleDesc inputDesc);
extern ExprState *ExecPrepareExpr(Expr *node, EState *estate);
extern ExprState *ExecPrepareQual(List *qual, EState *estate);
extern ExprState *ExecPrepareCheck(List *qual, EState *estate);
extern List *ExecPrepareExprList(List *nodes, EState *estate);

/*
 * ExecEvalExpr
 *
 * Evaluate expression identified by "state" in the execution context
 * given by "econtext".  *isNull is set to the is-null flag for the result,
 * and the Datum value is the function result.
 *
 * The caller should already have switched into the temporary memory
 * context econtext->ecxt_per_tuple_memory.  The convenience entry point
 * ExecEvalExprSwitchContext() is provided for callers who don't prefer to
 * do the switch in an outer loop.
 */
#ifndef FRONTEND
static inline Datum
ExecEvalExpr(ExprState *state,
			 ExprContext *econtext,
			 bool *isNull)
{
	return state->evalfunc(state, econtext, isNull);
}
#endif

/*
 * ExecEvalExprSwitchContext
 *
 * Same as ExecEvalExpr, but get into the right allocation context explicitly.
 */
#ifndef FRONTEND
static inline Datum
ExecEvalExprSwitchContext(ExprState *state,
						  ExprContext *econtext,
						  bool *isNull)
{
	Datum		retDatum;
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	retDatum = state->evalfunc(state, econtext, isNull);
	MemoryContextSwitchTo(oldContext);
	return retDatum;
}
#endif

/*
 * ExecProject
 *
 * Projects a tuple based on projection info and stores it in the slot passed
 * to ExecBuildProjectInfo().
 *
 * Note: the result is always a virtual tuple; therefore it may reference
 * the contents of the exprContext's scan tuples and/or temporary results
 * constructed in the exprContext.  If the caller wishes the result to be
 * valid longer than that data will be valid, he must call ExecMaterializeSlot
 * on the result slot.
 */
#ifndef FRONTEND
static inline TupleTableSlot *
ExecProject(ProjectionInfo *projInfo)
{
	ExprContext *econtext = projInfo->pi_exprContext;
	ExprState  *state = &projInfo->pi_state;
	TupleTableSlot *slot = state->resultslot;
	bool		isnull;

	/*
	 * Clear any former contents of the result slot.  This makes it safe for
	 * us to use the slot's Datum/isnull arrays as workspace.
	 */
	ExecClearTuple(slot);

	/* Run the expression, discarding scalar result from the last column. */
	(void) ExecEvalExprSwitchContext(state, econtext, &isnull);

	/*
	 * Successfully formed a result row.  Mark the result slot as containing a
	 * valid virtual tuple (inlined version of ExecStoreVirtualTuple()).
	 */
	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;

	return slot;
}
#endif

/*
 * ExecQual - evaluate a qual prepared with ExecInitQual (possibly via
 * ExecPrepareQual).  Returns true if qual is satisfied, else false.
 *
 * Note: ExecQual used to have a third argument "resultForNull".  The
 * behavior of this function now corresponds to resultForNull == false.
 * If you want the resultForNull == true behavior, see ExecCheck.
 */
#ifndef FRONTEND
static inline bool
ExecQual(ExprState *state, ExprContext *econtext)
{
	Datum		ret;
	bool		isnull;

	/* short-circuit (here and in ExecInitQual) for empty restriction list */
	if (state == NULL)
		return true;

	/* verify that expression was compiled using ExecInitQual */
	Assert(state->flags & EEO_FLAG_IS_QUAL);

	ret = ExecEvalExprSwitchContext(state, econtext, &isnull);

	/* EEOP_QUAL should never return NULL */
	Assert(!isnull);

	return DatumGetBool(ret);
}
#endif

/*
 * ExecQualAndReset() - evaluate qual with ExecQual() and reset expression
 * context.
 */
#ifndef FRONTEND
static inline bool
ExecQualAndReset(ExprState *state, ExprContext *econtext)
{
	bool		ret = ExecQual(state, econtext);

	/* inline ResetExprContext, to avoid ordering issue in this file */
	MemoryContextReset(econtext->ecxt_per_tuple_memory);
	return ret;
}
#endif

extern bool ExecCheck(ExprState *state, ExprContext *context);

/*
 * prototypes from functions in execSRF.c
 */
extern SetExprState *ExecInitTableFunctionResult(Expr *expr,
												 ExprContext *econtext, PlanState *parent);
extern Tuplestorestate *ExecMakeTableFunctionResult(SetExprState *setexpr,
													ExprContext *econtext,
													MemoryContext argContext,
													TupleDesc expectedDesc,
													bool randomAccess);
extern SetExprState *ExecInitFunctionResultSet(Expr *expr,
											   ExprContext *econtext, PlanState *parent);
extern Datum ExecMakeFunctionResultSet(SetExprState *fcache,
									   ExprContext *econtext,
									   MemoryContext argContext,
									   bool *isNull,
									   ExprDoneCond *isDone);

/*
 * prototypes from functions in execScan.c
 */
typedef TupleTableSlot *(*ExecScanAccessMtd) (ScanState *node);
typedef bool (*ExecScanRecheckMtd) (ScanState *node, TupleTableSlot *slot);

extern TupleTableSlot *ExecScan(ScanState *node, ExecScanAccessMtd accessMtd,
								ExecScanRecheckMtd recheckMtd);
extern void ExecAssignScanProjectionInfo(ScanState *node);
extern void ExecAssignScanProjectionInfoWithVarno(ScanState *node, Index varno);
extern void ExecScanReScan(ScanState *node);

/*
 * prototypes from functions in execTuples.c
 */
extern void ExecInitResultTypeTL(PlanState *planstate);
extern void ExecInitResultSlot(PlanState *planstate,
							   const TupleTableSlotOps *tts_ops);
extern void ExecInitResultTupleSlotTL(PlanState *planstate,
									  const TupleTableSlotOps *tts_ops);
extern void ExecInitScanTupleSlot(EState *estate, ScanState *scanstate,
								  TupleDesc tupleDesc,
								  const TupleTableSlotOps *tts_ops);
extern TupleTableSlot *ExecInitExtraTupleSlot(EState *estate,
											  TupleDesc tupledesc,
											  const TupleTableSlotOps *tts_ops);
extern TupleTableSlot *ExecInitNullTupleSlot(EState *estate, TupleDesc tupType,
											 const TupleTableSlotOps *tts_ops);
extern TupleDesc ExecTypeFromTL(List *targetList);
extern TupleDesc ExecCleanTypeFromTL(List *targetList);
extern TupleDesc ExecTypeFromExprList(List *exprList);
extern void ExecTypeSetColNames(TupleDesc typeInfo, List *namesList);
extern void UpdateChangedParamSet(PlanState *node, Bitmapset *newchg);

typedef struct TupOutputState
{
	TupleTableSlot *slot;
	DestReceiver *dest;
} TupOutputState;

extern TupOutputState *begin_tup_output_tupdesc(DestReceiver *dest,
												TupleDesc tupdesc,
												const TupleTableSlotOps *tts_ops);
extern void do_tup_output(TupOutputState *tstate, Datum *values, bool *isnull);
extern void do_text_output_multiline(TupOutputState *tstate, const char *txt);
extern void end_tup_output(TupOutputState *tstate);

/*
 * Write a single line of text given as a C string.
 *
 * Should only be used with a single-TEXT-attribute tupdesc.
 */
#define do_text_output_oneline(tstate, str_to_emit) \
	do { \
		Datum	values_[1]; \
		bool	isnull_[1]; \
		values_[0] = PointerGetDatum(cstring_to_text(str_to_emit)); \
		isnull_[0] = false; \
		do_tup_output(tstate, values_, isnull_); \
		pfree(DatumGetPointer(values_[0])); \
	} while (0)


/*
 * prototypes from functions in execUtils.c
 */
extern EState *CreateExecutorState(void);
extern void FreeExecutorState(EState *estate);
extern ExprContext *CreateExprContext(EState *estate);
extern ExprContext *CreateStandaloneExprContext(void);
extern void FreeExprContext(ExprContext *econtext, bool isCommit);
extern void ReScanExprContext(ExprContext *econtext);

#define ResetExprContext(econtext) \
	MemoryContextReset((econtext)->ecxt_per_tuple_memory)

extern ExprContext *MakePerTupleExprContext(EState *estate);

/* Get an EState's per-output-tuple exprcontext, making it if first use */
#define GetPerTupleExprContext(estate) \
	((estate)->es_per_tuple_exprcontext ? \
	 (estate)->es_per_tuple_exprcontext : \
	 MakePerTupleExprContext(estate))

#define GetPerTupleMemoryContext(estate) \
	(GetPerTupleExprContext(estate)->ecxt_per_tuple_memory)

/* Reset an EState's per-output-tuple exprcontext, if one's been created */
#define ResetPerTupleExprContext(estate) \
	do { \
		if ((estate)->es_per_tuple_exprcontext) \
			ResetExprContext((estate)->es_per_tuple_exprcontext); \
	} while (0)

extern void ExecAssignExprContext(EState *estate, PlanState *planstate);
extern TupleDesc ExecGetResultType(PlanState *planstate);
extern const TupleTableSlotOps *ExecGetResultSlotOps(PlanState *planstate,
													 bool *isfixed);
extern void ExecAssignProjectionInfo(PlanState *planstate,
									 TupleDesc inputDesc);
extern void ExecConditionalAssignProjectionInfo(PlanState *planstate,
												TupleDesc inputDesc, Index varno);
extern void ExecFreeExprContext(PlanState *planstate);
extern void ExecAssignScanType(ScanState *scanstate, TupleDesc tupDesc);
extern void ExecCreateScanSlotFromOuterPlan(EState *estate,
											ScanState *scanstate,
											const TupleTableSlotOps *tts_ops);

extern bool ExecRelationIsTargetRelation(EState *estate, Index scanrelid);

extern Relation ExecOpenScanRelation(EState *estate, Index scanrelid, int eflags);

extern void ExecInitRangeTable(EState *estate, List *rangeTable);

static inline RangeTblEntry *
exec_rt_fetch(Index rti, EState *estate)
{
	Assert(rti > 0 && rti <= estate->es_range_table_size);
	return estate->es_range_table_array[rti - 1];
}

extern Relation ExecGetRangeTableRelation(EState *estate, Index rti);

extern int	executor_errposition(EState *estate, int location);

extern void RegisterExprContextCallback(ExprContext *econtext,
										ExprContextCallbackFunction function,
										Datum arg);
extern void UnregisterExprContextCallback(ExprContext *econtext,
										  ExprContextCallbackFunction function,
										  Datum arg);

extern Datum GetAttributeByName(HeapTupleHeader tuple, const char *attname,
								bool *isNull);
extern Datum GetAttributeByNum(HeapTupleHeader tuple, AttrNumber attrno,
							   bool *isNull);

extern int	ExecTargetListLength(List *targetlist);
extern int	ExecCleanTargetListLength(List *targetlist);

extern TupleTableSlot *ExecGetTriggerOldSlot(EState *estate, ResultRelInfo *relInfo);
extern TupleTableSlot *ExecGetTriggerNewSlot(EState *estate, ResultRelInfo *relInfo);
extern TupleTableSlot *ExecGetReturningSlot(EState *estate, ResultRelInfo *relInfo);

/*
 * prototypes from functions in execIndexing.c
 */
extern void ExecOpenIndices(ResultRelInfo *resultRelInfo, bool speculative);
extern void ExecCloseIndices(ResultRelInfo *resultRelInfo);
extern List *ExecInsertIndexTuples(TupleTableSlot *slot, EState *estate, bool noDupErr,
								   bool *specConflict, List *arbiterIndexes);
extern bool ExecCheckIndexConstraints(TupleTableSlot *slot, EState *estate,
									  ItemPointer conflictTid, List *arbiterIndexes);
extern void check_exclusion_constraint(Relation heap, Relation index,
									   IndexInfo *indexInfo,
									   ItemPointer tupleid,
									   Datum *values, bool *isnull,
									   EState *estate, bool newIndex);

/*
 * prototypes from functions in execReplication.c
 */
extern bool RelationFindReplTupleByIndex(Relation rel, Oid idxoid,
										 LockTupleMode lockmode,
										 TupleTableSlot *searchslot,
										 TupleTableSlot *outslot);
extern bool RelationFindReplTupleSeq(Relation rel, LockTupleMode lockmode,
									 TupleTableSlot *searchslot, TupleTableSlot *outslot);

extern void ExecSimpleRelationInsert(EState *estate, TupleTableSlot *slot);
extern void ExecSimpleRelationUpdate(EState *estate, EPQState *epqstate,
									 TupleTableSlot *searchslot, TupleTableSlot *slot);
extern void ExecSimpleRelationDelete(EState *estate, EPQState *epqstate,
									 TupleTableSlot *searchslot);
extern void CheckCmdReplicaIdentity(Relation rel, CmdType cmd);

extern void CheckSubscriptionRelkind(char relkind, const char *nspname,
									 const char *relname);

#endif							/* EXECUTOR_H  */
