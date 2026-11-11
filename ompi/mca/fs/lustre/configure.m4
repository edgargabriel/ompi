# -*- shell-script -*-
#
# Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
#                         University Research and Technology
#                         Corporation.  All rights reserved.
# Copyright (c) 2004-2005 The University of Tennessee and The University
#                         of Tennessee Research Foundation.  All rights
#                         reserved.
# Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
#                         University of Stuttgart.  All rights reserved.
# Copyright (c) 2004-2005 The Regents of the University of California.
#                         All rights reserved.
# Copyright (c) 2010-2017 Cisco Systems, Inc.  All rights reserved
# Copyright (c) 2008-2012 University of Houston. All rights reserved.
# Copyright (c) 2022      Amazon.com, Inc. or its affiliates.  All Rights reserved.
# Copyright (c) 2026      Advanced Micro Devices, Inc. All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
# SPDX-License-Identifier: BSD-3-Clause-Open-MPI
#


# MCA_fs_lustre_CONFIG(action-if-can-compile,
#                        [action-if-cant-compile])
# ------------------------------------------------
AC_DEFUN([MCA_ompi_fs_lustre_CONFIG],[
    OPAL_VAR_SCOPE_PUSH([fs_lustre_happy])

    AC_CONFIG_FILES([ompi/mca/fs/lustre/Makefile])

    OMPI_CHECK_LUSTRE([fs_lustre],
                     [fs_lustre_happy="yes"],
                     [fs_lustre_happy="no"])

    AS_IF([test "$fs_lustre_happy" = "yes"],
          [opal_check_save_CPPFLAGS=$CPPFLAGS
           opal_check_save_LDFLAGS=$LDFLAGS
           opal_check_save_LIBS=$LIBS
           CPPFLAGS="$CPPFLAGS $fs_lustre_CPPFLAGS"
           LDFLAGS="$LDFLAGS $fs_lustre_LDFLAGS"
           LIBS="$LIBS $fs_lustre_LIBS"
           AC_CHECK_FUNC([llapi_layout_get_by_fd],
                         [AC_DEFINE([HAVE_LLAPI_LAYOUT_GET_BY_FD], [1],
                                    [Define to 1 if llapi_layout_get_by_fd is available])])
           CPPFLAGS=$opal_check_save_CPPFLAGS
           LDFLAGS=$opal_check_save_LDFLAGS
           LIBS=$opal_check_save_LIBS
           $1],
          [$2])

    # substitute in the things needed to build lustre
    AC_SUBST([fs_lustre_CPPFLAGS])
    AC_SUBST([fs_lustre_LDFLAGS])
    AC_SUBST([fs_lustre_LIBS])

    OPAL_VAR_SCOPE_POP
])dnl
