/* 
 * MINPACK-1 Least Squares Fitting Library
 *
 * Original public domain version by B. Garbow, K. Hillstrom, J. More'
 *   (Argonne National Laboratory, MINPACK project, March 1980)
 * 
 * Tranlation to C Language by S. Moshier (moshier.net)
 * 
 * Enhancements and packaging by C. Markwardt
 *   (comparable to IDL fitting routine MPFIT
 *    see http://cow.physics.wisc.edu/~craigm/idl/idl.html)
 */

/* Header file defining constants, data structures and functions of
   mpfit library 
   $Id: mpfit.h,v 1.16 2016/06/02 19:14:16 craigm Exp $
*/

#ifndef MPFIT_H
#define MPFIT_H

/* This is a C library.  Allow compilation with a C++ compiler */
#ifdef __cplusplus
extern "C" {
#endif

/* MPFIT version string */
#define MPFIT_VERSION "1.3"

/* Definition of a parameter constraint structure */
struct mp_par_struct {
  int fixed;        /* 1 = fixed; 0 = free */
  int limited[2];   /* 1 = low/upper limit; 0 = no limit */
  double limits[2]; /* lower/upper limit boundary value */

  char *parname;    /* Name of parameter, or 0 for none */
  double step;      /* Step size for finite difference */
  double relstep;   /* Relative step size for finite difference */
  int side;         /* Sidedness of finite difference derivative 
		        0 - one-sided derivative computed automatically
		        1 - one-sided derivative (f(x+h) - f(x)  )/h
		       -1 - one-sided derivative (f(x)   - f(x-h))/h
		        2 - two-sided derivative (f(x+h) - f(x-h))/(2*h) 
			3 - user-computed analytical derivatives
		    */
  int deriv_debug;  /* Derivative debug mode: 1 = Yes; 0 = No;

                       If yes, compute both analytical and numerical
                       derivatives and print them to the console for
                       comparison.

		       NOTE: when debugging, do *not* set side = 3,
		       but rather to the kind of numerical derivative
		       you want to compare the user-analytical one to
		       (0, 1, -1, or 2).
		    */
  double deriv_reltol; /* Relative tolerance for derivative debug
			  printout */
  double deriv_abstol; /* Absolute tolerance for derivative debug
			  printout */
};

/* Just a placeholder - do not use!! */
typedef void (*mp_iterproc)(void);

/* Definition of MPFIT configuration structure */
struct mp_config_struct {
  /* NOTE: the user may set the value explicitly; OR, if the passed
     value is zero, then the "Default" value will be substituted by
     mpfit(). */
  double ftol;    /* Relative chi-square convergence criterium Default: 1e-10 */
  double xtol;    /* Relative parameter convergence criterium  Default: 1e-10 */
  double gtol;    /* Orthogonality convergence criterium       Default: 1e-10 */
  double epsfcn;  /* Finite derivative step size               Default: MP_MACHEP0 */
  double stepfactor; /* Initial step bound                     Default: 100.0 */
  double covtol;  /* Range tolerance for covariance calculation Default: 1e-14 */
  int maxiter;    /* Maximum number of iterations.  If maxiter == MP_NO_ITER,
                     then basic error checking is done, and parameter
                     errors/covariances are estimated based on input
                     parameter values, but no fitting iterations are done. 
		     Default: 200
		  */
#define MP_NO_ITER (-1) /* No iterations, just checking */
  int maxfev;     /* Maximum number of function evaluations, or 0 for no limit
		     Default: 0 (no limit) */
  int nprint;     /* Default: 1 */
  int douserscale;/* Scale variables by user values?
		     1 = yes, user scale values in diag;
		     0 = no, variables scaled internally (Default) */
  int nofinitecheck; /* Disable check for infinite quantities from user?
			0 = do not perform check (Default)
			1 = perform check 
		     */
  mp_iterproc iterproc; /* Placeholder pointer - must set to 0 */

};

/* Definition of results structure, for when fit completes */
struct mp_result_struct {
  double bestnorm;     /* Final chi^2 */
  double orignorm;     /* Starting value of chi^2 */
  int niter;           /* Number of iterations */
  int nfev;            /* Number of function evaluations */
  int status;          /* Fitting status code */
  
  int npar;            /* Total number of parameters */
  int nfree;           /* Number of free parameters */
  int npegged;         /* Number of pegged parameters */
  int nfunc;           /* Number of residuals (= num. of data points) */

  double *resid;       /* Final residuals
			  nfunc-vector, or 0 if not desired */
  double *xerror;      /* Final parameter uncertainties (1-sigma)
			  npar-vector, or 0 if not desired */
  double *covar;       /* Final parameter covariance matrix
			  npar x npar array, or 0 if not desired */
  char version[20];    /* MPFIT version string */
};  

/* Convenience typedefs */  
typedef struct mp_par_struct mp_par;
typedef struct mp_config_struct mp_config;
typedef struct mp_result_struct mp_result;

/* Enforce type of fitting function */
typedef int (*mp_func)(int m, /* Number of functions (elts of fvec) */
		       int n, /* Number of variables (elts of x) */
		       double *x,      /* I - Parameters */
		       double *fvec,   /* O - function values */
		       double **dvec,  /* O - function derivatives (optional)*/
		       void *private_data); /* I/O - function private data*/

/* Error codes */
#define MP_ERR_INPUT (0)         /* General input parameter error */
#define MP_ERR_NAN (-16)         /* User function produced non-finite values */
#define MP_ERR_FUNC (-17)        /* No user function was supplied */
#define MP_ERR_NPOINTS (-18)     /* No user data points were supplied */
#define MP_ERR_NFREE (-19)       /* No free parameters */
#define MP_ERR_MEMORY (-20)      /* Memory allocation error */
#define MP_ERR_INITBOUNDS (-21)  /* Initial values inconsistent w constraints*/
#define MP_ERR_BOUNDS (-22)      /* Initial constraints inconsistent */
#define MP_ERR_PARAM (-23)       /* General input parameter error */
#define MP_ERR_DOF (-24)         /* Not enough degrees of freedom */

/* Potential success status codes */
#define MP_OK_CHI (1)            /* Convergence in chi-square value */
#define MP_OK_PAR (2)            /* Convergence in parameter value */
#define MP_OK_BOTH (3)           /* Both MP_OK_PAR and MP_OK_CHI hold */
#define MP_OK_DIR (4)            /* Convergence in orthogonality */
#define MP_MAXITER (5)           /* Maximum number of iterations reached */
#define MP_FTOL (6)              /* ftol is too small; no further improvement*/
#define MP_XTOL (7)              /* xtol is too small; no further improvement*/
#define MP_GTOL (8)              /* gtol is too small; no further improvement*/

/* Double precision numeric constants */
#define MP_MACHEP0 2.2204460e-16
#define MP_DWARF   2.2250739e-308
#define MP_GIANT   1.7976931e+308

#if 0
/* Float precision */
#define MP_MACHEP0 1.19209e-07
#define MP_DWARF   1.17549e-38
#define MP_GIANT   3.40282e+38
#endif

#define MP_RDWARF  (sqrt(MP_DWARF*1.5)*10)
#define MP_RGIANT  (sqrt(MP_GIANT)*0.1)


/* External function prototype declarations */
extern int mpfit(mp_func funct, int m, int npar,
		 double *xall, mp_par *pars, mp_config *config, 
		 void *private_data, 
		 mp_result *result);



/* C99 uses isfinite() instead of finite() */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L
#define mpfinite(x) isfinite(x)

/* Microsoft C uses _finite(x) instead of finite(x) */
#elif defined(_MSC_VER) && _MSC_VER
#include <float.h>
#define mpfinite(x) _finite(x)

/* Default is to assume that compiler/library has finite() function */
#else
#define mpfinite(x) finite(x)

#endif

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* MPFIT_H */
