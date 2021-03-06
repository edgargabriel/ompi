/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mpi/fortran/mpif-h/bindings.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak PMPI_WIN_SHARED_QUERY = ompi_win_shared_query_f
#pragma weak pmpi_win_shared_query = ompi_win_shared_query_f
#pragma weak pmpi_win_shared_query_ = ompi_win_shared_query_f
#pragma weak pmpi_win_shared_query__ = ompi_win_shared_query_f

#pragma weak PMPI_Win_shared_query_f = ompi_win_shared_query_f
#pragma weak PMPI_Win_shared_query_f08 = ompi_win_shared_query_f

#pragma weak PMPI_WIN_SHARED_QUERY_CPTR = ompi_win_shared_query_f
#pragma weak pmpi_win_shared_query_cptr = ompi_win_shared_query_f
#pragma weak pmpi_win_shared_query_cptr_ = ompi_win_shared_query_f
#pragma weak pmpi_win_shared_query_cptr__ = ompi_win_shared_query_f

#pragma weak PMPI_Win_shared_query_cptr_f = ompi_win_shared_query_f
#pragma weak PMPI_Win_shared_query_cptr_f08 = ompi_win_shared_query_f
#else
OMPI_GENERATE_F77_BINDINGS (PMPI_WIN_SHARED_QUERY,
                            pmpi_win_shared_query,
                            pmpi_win_shared_query_,
                            pmpi_win_shared_query__,
                            pompi_win_shared_query_f,
                            (MPI_Fint *win, MPI_Fint *rank, MPI_Aint *size,
                             MPI_Fint *disp_unit, char *baseptr,
                             MPI_Fint *ierr),
                            (win, rank, size, disp_unit, baseptr, ierr) )

OMPI_GENERATE_F77_BINDINGS (PMPI_WIN_SHARED_QUERY_CPTR,
                            pmpi_win_shared_query_cptr,
                            pmpi_win_shared_query_cptr_,
                            pmpi_win_shared_query_cptr__,
                            pompi_win_shared_query_cptr_f,
                            (MPI_Fint *win, MPI_Fint *rank, MPI_Aint *size,
                             MPI_Fint *disp_unit, char *baseptr,
                             MPI_Fint *ierr),
                            (win, rank, size, disp_unit, baseptr, ierr) )
#endif
#endif

#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_WIN_SHARED_QUERY = ompi_win_shared_query_f
#pragma weak mpi_win_shared_query = ompi_win_shared_query_f
#pragma weak mpi_win_shared_query_ = ompi_win_shared_query_f
#pragma weak mpi_win_shared_query__ = ompi_win_shared_query_f

#pragma weak MPI_Win_shared_query_f = ompi_win_shared_query_f
#pragma weak MPI_Win_shared_query_f08 = ompi_win_shared_query_f

#pragma weak MPI_WIN_SHARED_QUERY_CPTR = ompi_win_shared_query_f
#pragma weak mpi_win_shared_query_cptr = ompi_win_shared_query_f
#pragma weak mpi_win_shared_query_cptr_ = ompi_win_shared_query_f
#pragma weak mpi_win_shared_query_cptr__ = ompi_win_shared_query_f

#pragma weak MPI_Win_shared_query_cptr_f = ompi_win_shared_query_f
#pragma weak MPI_Win_shared_query_cptr_f08 = ompi_win_shared_query_f
#else
#if ! OMPI_BUILD_MPI_PROFILING
OMPI_GENERATE_F77_BINDINGS (MPI_WIN_SHARED_QUERY,
                            mpi_win_shared_query,
                            mpi_win_shared_query_,
                            mpi_win_shared_query__,
                            ompi_win_shared_query_f,
                            (MPI_Fint *win, MPI_Fint *rank, MPI_Aint *size,
                             MPI_Fint *disp_unit, char *baseptr,
                             MPI_Fint *ierr),
                            (win, rank, size, disp_unit, baseptr, ierr) )

OMPI_GENERATE_F77_BINDINGS (MPI_WIN_SHARED_QUERY_CPTR,
                            mpi_win_shared_query_cptr,
                            mpi_win_shared_query_cptr_,
                            mpi_win_shared_query_cptr__,
                            ompi_win_shared_query_cptr_f,
                            (MPI_Fint *win, MPI_Fint *rank, MPI_Aint *size,
                             MPI_Fint *disp_unit, char *baseptr,
                             MPI_Fint *ierr),
                            (win, rank, size, disp_unit, baseptr, ierr) )
#else
#define ompi_win_shared_query_f pompi_win_shared_query_f
#define ompi_win_shared_query_cptr_f pompi_win_shared_query_cptr_f
#endif
#endif


void ompi_win_shared_query_f(MPI_Fint *win, MPI_Fint *rank, MPI_Aint *size,
                             MPI_Fint *disp_unit, char *baseptr,
                             MPI_Fint *ierr)
{
    int c_ierr;
    MPI_Win c_win;
    OMPI_SINGLE_NAME_DECL(disp_unit);

    c_win = PMPI_Win_f2c(*win);

    c_ierr = PMPI_Win_shared_query(c_win, OMPI_FINT_2_INT(*rank), size,
                                   OMPI_SINGLE_NAME_CONVERT(disp_unit), baseptr);
    if (NULL != ierr) *ierr = OMPI_INT_2_FINT(c_ierr);

    if (MPI_SUCCESS == c_ierr) {
        OMPI_SINGLE_INT_2_FINT(disp_unit);
    }
}

/*
 * Note that MPI-3 mandates a second form of the MPI_Win_shared_query
 * interface -- one that has a "_cptr" suffix.
 */
void ompi_win_shared_query_cptr_f(MPI_Fint *win, MPI_Fint *rank, MPI_Aint *size,
                                  MPI_Fint *disp_unit, char *baseptr,
                                  MPI_Fint *ierr)
{
    ompi_win_shared_query_f(win, rank, size, disp_unit, baseptr, ierr);
}

