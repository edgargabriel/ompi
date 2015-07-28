/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2015 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2011 University of Houston. All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */


#include "ompi_config.h"
#include "fs_gpfs.h"
#include <unistd.h>

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/fs/fs.h"

/*
 *	file_delete_gpfs
 *
 *	Function:	- deletes a file
 *	Accepts:	- file name & info
 *	Returns:	- Success if file closed
 */
int
mca_fs_gpfs_file_delete (char *filename,
                              struct ompi_info_t *info)
{
	//DEBUG: fprintf(stderr, "Using mca_fs_gpfs_file_delete to delete a file.\n");
    int ret = OMPI_SUCCESS;

    ret = unlink(filename);

    if (0 > ret) {
        return OMPI_ERROR;
    }

    return OMPI_SUCCESS;
}
