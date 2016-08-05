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
 * Copyright (c) 2008-2016 University of Houston. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef IO_OMPIO_LOGGING_H
#define IO_OMPIO_LOGGING_H

#include "ompi/communicator/communicator.h"


#define IO_OMPIO_LOG_EVENT_OPEN      100
#define IO_OMPIO_LOG_EVENT_FVIEW_SET 101
#define IO_OMPIO_LOG_EVENT_UPDATE    102
#define IO_OMPIO_LOG_EVENT_CLOSE     103

#define IO_OMPIO_LOG_WRITE 1
#define IO_OMPIO_LOG_READ  2
#define IO_OMPIO_LOG_SEEK  3

#define IO_OMPIO_LOG_MAX_FILENAME_LEN  1024
#define IO_OMPIO_LOG_MAX_BUF_LEN  65536

OMPI_DECLSPEC void io_ompio_log ( mca_io_ompio_file_t *ompio_file, int event_type, ...);



#endif
