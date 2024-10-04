/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2021 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2024      Triad National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2024      Advanced Micro Devices, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "fcoll_vulcan.h"
#include "fcoll_vulcan_internal.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/fcoll/fcoll.h"
#include "ompi/mca/fcoll/base/fcoll_base_coll_array.h"
#include "ompi/mca/common/ompio/common_ompio.h"
#include "ompi/mca/common/ompio/common_ompio_buffer.h"
#include "ompi/mca/io/io.h"
#include "ompi/mca/common/ompio/common_ompio_request.h"
#include "math.h"
#include "ompi/mca/pml/pml.h"
#include "opal/mca/accelerator/accelerator.h"
#include <unistd.h>

#define DEBUG_ON 0
#define NOT_AGGR_INDEX -1

typedef struct mca_io_ompio_aggregator_data {
    int *disp_index, *sorted, n;
    size_t *fview_count;
    int *max_disp_index;
    int **blocklen_per_process;
    MPI_Aint **displs_per_process, total_bytes, bytes_per_cycle, total_bytes_read;
    MPI_Comm comm;
    char *global_buf, *prev_global_buf;
    ompi_datatype_t **sendtype, **prev_sendtype;
    struct iovec *global_iov_array;
    int current_index, current_position;
    int bytes_to_read_in_cycle, bytes_remaining, procs_per_group;
    int *procs_in_group, iov_index;
    int bytes_recvd, prev_bytes_recvd;
    struct iovec *decoded_iov;
    int bytes_to_read, prev_bytes_to_read;
} mca_io_ompio_aggregator_data;


#define SWAP_REQUESTS(_r1,_r2) { \
    ompi_request_t **_t=_r1;     \
    _r1=_r2;                     \
    _r2=_t;}

#define SWAP_AGGR_POINTERS(_aggr,_num) {                        \
    int _i;                                                     \
    char *_t;                                                   \
    for (_i=0; _i<_num; _i++ ) {                                \
        _aggr[_i]->prev_bytes_recvd=_aggr[_i]->bytes_recvd;         \
        _aggr[_i]->prev_bytes_to_read=_aggr[_i]->bytes_to_read; \
        _t=_aggr[_i]->prev_global_buf;                            \
        _aggr[_i]->prev_global_buf=_aggr[_i]->global_buf;         \
        _aggr[_i]->global_buf=_t;                                 \
        _t=(char *)_aggr[_i]->sendtype;                           \
        _aggr[_i]->sendtype=_aggr[_i]->prev_sendtype;             \
        _aggr[_i]->prev_sendtype=(ompi_datatype_t **)_t;          }                                                             \
}

static int shuffle_init (int index, int cycles, int aggregator, int rank,
                         mca_io_ompio_aggregator_data *data, ompi_request_t **reqs );
static int read_init (ompio_file_t *fh, int index, int cycles, int aggregator, int rank,
		      mca_io_ompio_aggregator_data *aggr_data,
                      int read_syncType, ompi_request_t **request,
                      bool is_accelerator_buffer);

int mca_fcoll_vulcan_file_read_all (struct ompio_file_t *fh,
                                    void *buf,
                                    size_t count,
                                    struct ompi_datatype_t *datatype,
                                    ompi_status_public_t *status)
{
    int index = 0;
    int cycles = 0;
    int ret =0, l, i, j, bytes_per_cycle;
    uint32_t iov_count = 0;
    struct iovec *decoded_iov = NULL;
    struct iovec *local_iov_array=NULL;
    uint32_t total_fview_count = 0;
    int local_count = 0;
    ompi_request_t **reqs = NULL;
    ompi_request_t *req_iread = MPI_REQUEST_NULL;
    mca_io_ompio_aggregator_data **aggr_data=NULL;

    ptrdiff_t *displs = NULL;
    int vulcan_num_io_procs;
    size_t max_data = 0;

    struct iovec **broken_iov_arrays=NULL;
    struct iovec **broken_decoded_iovs=NULL;
    int *broken_counts=NULL;
    int *broken_iov_counts=NULL;
    MPI_Aint *broken_total_lengths=NULL;

    int aggr_index = NOT_AGGR_INDEX;
    int read_sync_type = 2;
    int *result_counts=NULL;

    ompi_count_array_t fview_count_desc;
    ompi_disp_array_t displs_desc;
    int is_gpu, is_managed;
    bool use_accelerator_buffer = false;

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    double read_time = 0.0, start_read_time = 0.0, end_read_time = 0.0;
    double comm_time = 0.0, start_comm_time = 0.0, end_comm_time = 0.0;
    double exch_read = 0.0, start_exch = 0.0, end_exch = 0.0;
    mca_common_ompio_print_entry nentry;
#endif

    /**************************************************************************
     ** 1.  In case the data is not contiguous in memory, decode it into an iovec
     **************************************************************************/
    vulcan_num_io_procs = fh->f_get_mca_parameter_value ( "num_aggregators", strlen ("num_aggregators"));
    if (OMPI_ERR_MAX == vulcan_num_io_procs) {
        ret = OMPI_ERROR;
        goto exit;
    }
    bytes_per_cycle = fh->f_bytes_per_agg;

    if ((1 == mca_fcoll_vulcan_async_io) && (NULL == fh->f_fbtl->fbtl_ipreadv)) {
        opal_output (1, "vulcan_read_all: fbtl Does NOT support ipreadv() (asynchronous read) \n");
        ret = MPI_ERR_UNSUPPORTED_OPERATION;
        goto exit;
    }

    mca_common_ompio_check_gpu_buf (fh, buf, &is_gpu, &is_managed);
    if (is_gpu && !is_managed &&
        fh->f_get_mca_parameter_value ("use_accelerator_buffers", strlen("use_accelerator_buffers"))) {
        use_accelerator_buffer = true;
    }
    /* since we want to overlap 2 iterations, define the bytes_per_cycle to be half of what
       the user requested */
    bytes_per_cycle = bytes_per_cycle/2;

    ret = mca_common_ompio_decode_datatype ((struct ompio_file_t *) fh,
                                            datatype, count, buf, &max_data,
                                            fh->f_mem_convertor, &decoded_iov,
                                            &iov_count);
    if (OMPI_SUCCESS != ret){
        goto exit;
    }

    if (MPI_STATUS_IGNORE != status) {
        status->_ucount = max_data;
    }

    ret = mca_fcoll_vulcan_get_configuration (fh, vulcan_num_io_procs, mca_fcoll_vulcan_num_groups, max_data);
    if (OMPI_SUCCESS != ret){
        goto exit;
    }

    aggr_data = (mca_io_ompio_aggregator_data **) malloc (fh->f_num_aggrs *
                                                          sizeof(mca_io_ompio_aggregator_data*));
    if (NULL == aggr_data) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    for (i = 0; i < fh->f_num_aggrs; i++) {
        // At this point we know the number of aggregators. If there is a correlation between
        // number of aggregators and number of IO nodes, we know how many aggr_data arrays we need
        // to allocate.
        aggr_data[i] = (mca_io_ompio_aggregator_data *) calloc (1, sizeof(mca_io_ompio_aggregator_data));
        aggr_data[i]->procs_per_group = fh->f_procs_per_group;
        aggr_data[i]->procs_in_group  = fh->f_procs_in_group;
        aggr_data[i]->comm = fh->f_comm;
        // Identify if the process is an aggregator.
        // If so, aggr_index would be its index in "aggr_data" and "aggregators" arrays.
        if (fh->f_aggr_list[i] == fh->f_rank) {
            aggr_index = i;
        }
    }

    /*********************************************************************
     *** 2. Generate the local offsets/lengths array corresponding to
     ***    this read operation
     ********************************************************************/
    ret = fh->f_generate_current_file_view ((struct ompio_file_t *) fh,
                                            max_data, &local_iov_array,
                                            &local_count);
    if (ret != OMPI_SUCCESS) {
        goto exit;
    }

    /*************************************************************************
     ** 2b. Separate the local_iov_array entries based on the number of aggregators
     *************************************************************************/
    // Modifications for the even distribution:
    long domain_size;
    ret = mca_fcoll_vulcan_minmax (fh, local_iov_array, local_count, fh->f_num_aggrs, &domain_size);

    // broken_iov_arrays[0] contains broken_counts[0] entries to aggregator 0,
    // broken_iov_arrays[1] contains broken_counts[1] entries to aggregator 1, etc.
    ret = mca_fcoll_vulcan_break_file_view (decoded_iov, iov_count,
                                            local_iov_array, local_count,
                                            &broken_decoded_iovs, &broken_iov_counts,
                                            &broken_iov_arrays, &broken_counts,
                                            &broken_total_lengths,
                                            fh->f_num_aggrs, domain_size);

    /**************************************************************************
     ** 3. Determine the total amount of data to be read and no. of cycles
     **************************************************************************/
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    start_comm_time = MPI_Wtime();
#endif
    ret = fh->f_comm->c_coll->coll_allreduce (MPI_IN_PLACE, broken_total_lengths,
                                              fh->f_num_aggrs, MPI_LONG, MPI_SUM,
                                              fh->f_comm,
                                              fh->f_comm->c_coll->coll_allreduce_module);
    if (OMPI_SUCCESS != ret) {
        goto exit;
    }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_comm_time = MPI_Wtime();
    comm_time += (end_comm_time - start_comm_time);
#endif

    cycles=0;
    for (i = 0; i < fh->f_num_aggrs; i++) {
#if DEBUG_ON
        printf("%d: Overall broken_total_lengths[%d] = %ld\n", fh->f_rank, i, broken_total_lengths[i]);
#endif
        if (ceil((double)broken_total_lengths[i]/bytes_per_cycle) > cycles) {
            cycles = ceil((double)broken_total_lengths[i]/bytes_per_cycle);
        }
    }

    result_counts = (int *) malloc (fh->f_num_aggrs * fh->f_procs_per_group * sizeof(int));
    if (NULL == result_counts) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    start_comm_time = MPI_Wtime();
#endif
    ret = fh->f_comm->c_coll->coll_allgather (broken_counts, fh->f_num_aggrs, MPI_INT,
                                              result_counts, fh->f_num_aggrs, MPI_INT,
                                              fh->f_comm,
					      fh->f_comm->c_coll->coll_allgather_module);
    if (OMPI_SUCCESS != ret) {
        goto exit;
    }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_comm_time = MPI_Wtime();
    comm_time += (end_comm_time - start_comm_time);
#endif

    /*************************************************************
     *** 4. Allgather the offset/lengths array from all processes
     *************************************************************/
    for (i = 0; i < fh->f_num_aggrs; i++) {
        aggr_data[i]->total_bytes = broken_total_lengths[i];
        aggr_data[i]->decoded_iov = broken_decoded_iovs[i];
        aggr_data[i]->fview_count = (size_t *)malloc (fh->f_procs_per_group * sizeof (size_t));
        if (NULL == aggr_data[i]->fview_count) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }

        for (j = 0; j < fh->f_procs_per_group; j++) {
            aggr_data[i]->fview_count[j] = result_counts[fh->f_num_aggrs*j+i];
        }

        displs = (ptrdiff_t *)malloc (fh->f_procs_per_group * sizeof (ptrdiff_t));
        if (NULL == displs) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }

        displs[0] = 0;
        total_fview_count = (uint32_t) aggr_data[i]->fview_count[0];
        for (j = 1 ; j < fh->f_procs_per_group ; j++) {
            total_fview_count += aggr_data[i]->fview_count[j];
            displs[j] = displs[j-1] + aggr_data[i]->fview_count[j-1];
        }

#if DEBUG_ON
        printf("total_fview_count : %d\n", total_fview_count);
        if (fh->f_aggr_list[i] == fh->f_rank) {
            for (j=0 ; j<fh->f_procs_per_group ; i++) {
                printf ("%d: PROCESS: %d  ELEMENTS: %ld  DISPLS: %ld\n",
                        fh->f_rank, j,
                        aggr_data[i]->fview_count[j],
                        displs[j]);
            }
        }
#endif

        /* allocate the global iovec  */
        if (0 != total_fview_count) {
            aggr_data[i]->global_iov_array = (struct iovec*) malloc (total_fview_count *
                                                                     sizeof(struct iovec));
            if (NULL == aggr_data[i]->global_iov_array) {
                opal_output(1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
        start_comm_time = MPI_Wtime();
#endif
        OMPI_COUNT_ARRAY_INIT(&fview_count_desc, aggr_data[i]->fview_count);
        OMPI_DISP_ARRAY_INIT(&displs_desc, displs);
        ret = fh->f_comm->c_coll->coll_allgatherv (broken_iov_arrays[i],
                                                   broken_counts[i],
                                                   fh->f_iov_type,
                                                   aggr_data[i]->global_iov_array,
                                                   fview_count_desc,
                                                   displs_desc,
                                                   fh->f_iov_type,
                                                   fh->f_comm,
                                                   fh->f_comm->c_coll->coll_allgatherv_module );
        if (OMPI_SUCCESS != ret) {
            goto exit;
        }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
        end_comm_time = MPI_Wtime();
        comm_time += (end_comm_time - start_comm_time);
#endif

        /****************************************************************************************
         *** 5. Sort the global offset/lengths list based on the offsets.
         *** The result of the sort operation is the 'sorted', an integer array,
         *** which contains the indexes of the global_iov_array based on the offset.
         *** For example, if global_iov_array[x].offset is followed by global_iov_array[y].offset
         *** in the file, and that one is followed by global_iov_array[z].offset, than
         *** sorted[0] = x, sorted[1]=y and sorted[2]=z;
         ******************************************************************************************/
        if (0 != total_fview_count) {
            aggr_data[i]->sorted = (int *)malloc (total_fview_count * sizeof(int));
            if (NULL == aggr_data[i]->sorted) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
            ompi_fcoll_base_sort_iovec (aggr_data[i]->global_iov_array, total_fview_count,
					aggr_data[i]->sorted);
        }

        if (NULL != local_iov_array) {
            free(local_iov_array);
            local_iov_array = NULL;
        }

        if (NULL != displs) {
            free(displs);
            displs=NULL;
        }

#if DEBUG_ON
        if (fh->f_aggr_list[i] == fh->f_rank) {
            uint32_t tv=0;
            for (tv = 0 ; tv < total_fview_count ; tv++) {
                printf("%d: OFFSET: %lu   LENGTH: %ld\n",
                       fh->f_rank,
                       (uint64_t)aggr_data[i]->global_iov_array[aggr_data[i]->sorted[tv]].iov_base,
                       aggr_data[i]->global_iov_array[aggr_data[i]->sorted[tv]].iov_len);
            }
        }
#endif
        /*************************************************************
         *** 6. Determine the number of cycles required to execute this
         ***    operation
         *************************************************************/
        aggr_data[i]->bytes_per_cycle = bytes_per_cycle;

        if (fh->f_aggr_list[i] == fh->f_rank) {
            aggr_data[i]->disp_index = (int *)malloc (fh->f_procs_per_group * sizeof (int));
            if (NULL == aggr_data[i]->disp_index) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }

            aggr_data[i]->max_disp_index = (int *)calloc (fh->f_procs_per_group,  sizeof (int));
            if (NULL == aggr_data[i]->max_disp_index) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }

            aggr_data[i]->blocklen_per_process = (int **)calloc (fh->f_procs_per_group, sizeof (int*));
            if (NULL == aggr_data[i]->blocklen_per_process) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }

            aggr_data[i]->displs_per_process = (MPI_Aint **)calloc (fh->f_procs_per_group, sizeof (MPI_Aint*));
            if (NULL == aggr_data[i]->displs_per_process) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }

            if (use_accelerator_buffer) {
                opal_output_verbose(10, ompi_fcoll_base_framework.framework_output,
                                    "Allocating GPU device buffer for aggregation\n");
                ret = opal_accelerator.mem_alloc(MCA_ACCELERATOR_NO_DEVICE_ID, (void**)&aggr_data[i]->global_buf,
                                                 bytes_per_cycle);
                if (OPAL_SUCCESS != ret) {
                    opal_output(1, "Could not allocate accelerator memory");
                    ret = OMPI_ERR_OUT_OF_RESOURCE;
                    goto exit;
                }
                ret = opal_accelerator.mem_alloc(MCA_ACCELERATOR_NO_DEVICE_ID, (void**)&aggr_data[i]->prev_global_buf,
                                                 bytes_per_cycle);
                if (OPAL_SUCCESS != ret) {
                    opal_output(1, "Could not allocate accelerator memory");
                    ret = OMPI_ERR_OUT_OF_RESOURCE;
                    goto exit;
                }
            } else {
                aggr_data[i]->global_buf       = (char *) malloc (bytes_per_cycle);
                aggr_data[i]->prev_global_buf  = (char *) malloc (bytes_per_cycle);
                if (NULL == aggr_data[i]->global_buf || NULL == aggr_data[i]->prev_global_buf){
                    opal_output(1, "OUT OF MEMORY");
                    ret = OMPI_ERR_OUT_OF_RESOURCE;
                    goto exit;
                }
            }

            aggr_data[i]->sendtype = (ompi_datatype_t **) malloc (fh->f_procs_per_group  *
                                                                  sizeof(ompi_datatype_t *));
            aggr_data[i]->prev_sendtype = (ompi_datatype_t **) malloc (fh->f_procs_per_group  *
                                                                       sizeof(ompi_datatype_t *));
            if (NULL == aggr_data[i]->sendtype || NULL == aggr_data[i]->prev_sendtype) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
            for(l=0;l<fh->f_procs_per_group;l++){
                aggr_data[i]->sendtype[l]      = MPI_DATATYPE_NULL;
                aggr_data[i]->prev_sendtype[l] = MPI_DATATYPE_NULL;
            }
        }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
        start_exch = MPI_Wtime();
#endif
    }

    reqs = (ompi_request_t **)malloc ((fh->f_procs_per_group + 1 )*fh->f_num_aggrs *sizeof(ompi_request_t *));
    if (NULL == reqs) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    for (l=0,i=0; i < fh->f_num_aggrs; i++) {
        for ( j=0; j< (fh->f_procs_per_group+1); j++ ) {
            reqs[l] = MPI_REQUEST_NULL;
            l++;
        }
    }

    if( (1 == mca_fcoll_vulcan_async_io) ||
        ( (0 == mca_fcoll_vulcan_async_io) && (NULL != fh->f_fbtl->fbtl_ipreadv) && (2 < cycles))) {
        read_sync_type = 1;
    }

    if (cycles > 0) {
        if (NOT_AGGR_INDEX != aggr_index) {
	    // Register progress function that should be used by ompi_request_wait
	    mca_common_ompio_register_progress ();

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            start_read_time = MPI_Wtime();
#endif
            ret = read_init (fh, 0, cycles, fh->f_aggr_list[aggr_index], fh->f_rank,
			     aggr_data[aggr_index],
                             read_sync_type, &req_iread, use_accelerator_buffer);
            if (OMPI_SUCCESS != ret) {
                goto exit;
            }

            ret = ompi_request_wait(&req_iread, MPI_STATUS_IGNORE);
            if (OMPI_SUCCESS != ret){
                goto exit;
            }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            end_read_time = MPI_Wtime();
            read_time += end_read_time - start_read_time;
#endif
	}
    }

    for (index = 1; index < cycles; index++) {

        for (i = 0; i < fh->f_num_aggrs; i++) {
            ret = shuffle_init (index-1, cycles, fh->f_aggr_list[i], fh->f_rank, aggr_data[i],
                                &reqs[i*(fh->f_procs_per_group + 1)] );
            if (OMPI_SUCCESS != ret) {
                goto exit;
            }
        }

        SWAP_AGGR_POINTERS(aggr_data, fh->f_num_aggrs);
        if(NOT_AGGR_INDEX != aggr_index) {
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            start_read_time = MPI_Wtime();
#endif
            ret = read_init (fh, index, cycles, fh->f_aggr_list[aggr_index], fh->f_rank,
			     aggr_data[aggr_index], read_sync_type,
			     &req_iread, use_accelerator_buffer);
            if (OMPI_SUCCESS != ret){
                goto exit;
            }
        }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
	end_read_time = MPI_Wtime();
	read_time += end_read_time - start_read_time;
#endif

	ret = ompi_request_wait_all ((fh->f_procs_per_group + 1 )*fh->f_num_aggrs,
                                     reqs, MPI_STATUS_IGNORE);
        if (OMPI_SUCCESS != ret){
            goto exit;
        }

        if (NOT_AGGR_INDEX != aggr_index) {
            ret = ompi_request_wait (&req_iread, MPI_STATUS_IGNORE);
            if (OMPI_SUCCESS != ret){
                goto exit;
            }
        }
    } /* end  for (index = 0; index < cycles; index++) */

    if (cycles > 0) {
        for (i = 0; i < fh->f_num_aggrs; i++) {
            ret = shuffle_init (index-1, cycles, fh->f_aggr_list[i], fh->f_rank, aggr_data[i],
                                &reqs[i*(fh->f_procs_per_group + 1)] );
            if (OMPI_SUCCESS != ret) {
                goto exit;
            }
        }
	ret = ompi_request_wait_all ((fh->f_procs_per_group + 1 )*fh->f_num_aggrs,
                                     reqs, MPI_STATUS_IGNORE);
        if (OMPI_SUCCESS != ret){
            goto exit;
        }
    }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_exch = MPI_Wtime();
    exch_read += end_exch - start_exch;
    nentry.time[0] = read_time;
    nentry.time[1] = comm_time;
    nentry.time[2] = exch_read;
    nentry.aggregator = 0;
    for ( i=0; i<fh->f_num_aggrs; i++ ) {
        if (fh->f_aggr_list[i] == fh->f_rank)
        nentry.aggregator = 1;
    }
    nentry.nprocs_for_coll = fh->f_num_aggrs;
    if (!mca_common_ompio_full_print_queue(fh->f_coll_read_time)){
        mca_common_ompio_register_print_entry(fh->f_coll_read_time,
                                               nentry);
    }
#endif

exit :
    if (NULL != aggr_data) {

        for (i = 0; i < fh->f_num_aggrs; i++) {
            if (fh->f_aggr_list[i] == fh->f_rank) {
                if (NULL != aggr_data[i]->sendtype){
                    for (j = 0; j < aggr_data[i]->procs_per_group; j++) {
                        if (MPI_DATATYPE_NULL != aggr_data[i]->sendtype[j]) {
                            ompi_datatype_destroy(&aggr_data[i]->sendtype[j]);
                        }
                        if (MPI_DATATYPE_NULL != aggr_data[i]->prev_sendtype[j]) {
                            ompi_datatype_destroy(&aggr_data[i]->prev_sendtype[j]);
                        }
                    }
                    free(aggr_data[i]->sendtype);
                    free(aggr_data[i]->prev_sendtype);
                }

                free (aggr_data[i]->disp_index);
                free (aggr_data[i]->max_disp_index);
                if (use_accelerator_buffer) {
                    opal_accelerator.mem_release(MCA_ACCELERATOR_NO_DEVICE_ID, aggr_data[i]->global_buf);
                    opal_accelerator.mem_release(MCA_ACCELERATOR_NO_DEVICE_ID, aggr_data[i]->prev_global_buf);
                } else {
                    free (aggr_data[i]->global_buf);
                    free (aggr_data[i]->prev_global_buf);
                }
                for (l = 0;l < aggr_data[i]->procs_per_group; l++) {
                    free (aggr_data[i]->blocklen_per_process[l]);
                    free (aggr_data[i]->displs_per_process[l]);
                }

                free (aggr_data[i]->blocklen_per_process);
                free (aggr_data[i]->displs_per_process);
            }
            free (aggr_data[i]->sorted);
            free (aggr_data[i]->global_iov_array);
            free (aggr_data[i]->fview_count);
            free (aggr_data[i]->decoded_iov);

            free (aggr_data[i]);
        }
        free (aggr_data);
    }
    free(displs);
    free(decoded_iov);
    free(broken_counts);
    free(broken_total_lengths);
    free(broken_iov_counts);
    free(broken_decoded_iovs); // decoded_iov arrays[i] were freed as aggr_data[i]->decoded_iov;
    if (NULL != broken_iov_arrays) {
        for (i = 0; i < fh->f_num_aggrs; i++) {
            free(broken_iov_arrays[i]);
        }
    }
    free(broken_iov_arrays);
    free(fh->f_procs_in_group);
    free(fh->f_aggr_list);
    fh->f_procs_in_group=NULL;
    fh->f_procs_per_group=0;
    fh->f_aggr_list=NULL;
    free(result_counts);
    free(reqs);

    return OMPI_SUCCESS;
}

static int read_init (ompio_file_t *fh, int index, int cycles, int aggregator, int rank,
		      mca_io_ompio_aggregator_data *data,
                      int read_syncType, ompi_request_t **request,
                      bool is_accelerator_buffer)
{
    int ret = OMPI_SUCCESS;
    ssize_t ret_temp = 0;
    mca_ompio_request_t *ompio_req = NULL;
    int i, j, l;
    int blocks=0;
    int bytes_recvd = 0;
    int entries_per_aggregator=0;
    mca_io_ompio_local_io_array *file_offsets_for_agg=NULL;
    int temp_index=0, temp_pindex;
    MPI_Aint *memory_displacements=NULL;
    int *temp_disp_index=NULL;
    int* blocklength_proc=NULL;
    ptrdiff_t* displs_proc=NULL;
    int *sorted_file_offsets=NULL;

    /**********************************************************************
     ***  7a. Getting ready for next cycle: initializing and freeing buffers
     **********************************************************************/
    data->bytes_recvd = 0;

    if (aggregator == rank) {
	if (NULL != data->sendtype){
	    for (i = 0; i < data->procs_per_group; i++) {
		if (MPI_DATATYPE_NULL != data->sendtype[i]) {
		    ompi_datatype_destroy(&data->sendtype[i]);
		    data->sendtype[i] = MPI_DATATYPE_NULL;
		}
	    }
	}

	for (l = 0; l < data->procs_per_group; l++) {
	    data->disp_index[l] = 0;

	    if (data->max_disp_index[l] == 0) {
		data->blocklen_per_process[l] = (int *) calloc (INIT_LEN, sizeof(int));
		data->displs_per_process[l] = (MPI_Aint *) calloc (INIT_LEN, sizeof(MPI_Aint));
		if (NULL == data->displs_per_process[l] || NULL == data->blocklen_per_process[l]){
		    opal_output (1, "OUT OF MEMORY for displs\n");
		    ret = OMPI_ERR_OUT_OF_RESOURCE;
		    goto exit;
		}
		data->max_disp_index[l] = INIT_LEN;
	    } else {
		memset (data->blocklen_per_process[l], 0, data->max_disp_index[l]*sizeof(int));
		memset (data->displs_per_process[l], 0, data->max_disp_index[l]*sizeof(MPI_Aint));
	    }
	}
    } /* rank == aggregator */

    /**************************************************************************
     ***  7b. Determine the number of bytes to be actually read in this cycle
     **************************************************************************/
    int local_cycles= ceil((double)data->total_bytes / data->bytes_per_cycle);
    if (index  < (local_cycles -1)) {
        data->bytes_to_read_in_cycle = data->bytes_per_cycle;
    } else if ( index == (local_cycles -1)) {
        data->bytes_to_read_in_cycle = data->total_bytes - data->bytes_per_cycle*index;
    } else {
        data->bytes_to_read_in_cycle = 0;
    }
    data->bytes_to_read = data->bytes_to_read_in_cycle;

#if DEBUG_ON
    if (aggregator == rank) {
        printf ("****%d: CYCLE %d   Bytes %d**********\n",
                rank,
                index,
                data->bytes_to_read_in_cycle);
    }
#endif

    /*****************************************************************
     *** 7c. Calculate how much data will be sent to each process in
     *** this cycle
     *****************************************************************/

    /* The blocklen and displs calculation only done at aggregators */
    while (data->bytes_to_read_in_cycle) {
        /* This next block identifies which process is the holder
        ** of the sorted[current_index] element;
        */
        blocks = data->fview_count[0];
        for (j = 0 ; j < data->procs_per_group ; j++) {
            if (data->sorted[data->current_index] < blocks) {
                data->n = j;
                break;
            } else {
                blocks += data->fview_count[j+1];
            }
        }

        if (data->bytes_remaining) {
            /* Finish up a partially used buffer from the previous cycle */

            if (data->bytes_remaining <= data->bytes_to_read_in_cycle) {
                /* The data fits completely into the block */
                if (aggregator == rank) {
                    data->blocklen_per_process[data->n][data->disp_index[data->n]] = data->bytes_remaining;
                    data->displs_per_process[data->n][data->disp_index[data->n]] =
                        (ptrdiff_t)data->global_iov_array[data->sorted[data->current_index]].iov_base +
                        (data->global_iov_array[data->sorted[data->current_index]].iov_len
                         - data->bytes_remaining);

                    data->disp_index[data->n] += 1;

                    /* In this cases the length is consumed so allocating for
                       next displacement and blocklength*/
                    if (data->disp_index[data->n] == data->max_disp_index[data->n]) {
                        data->max_disp_index[data->n] *= 2;

                        data->blocklen_per_process[data->n] = (int *) realloc
                            ((void *)data->blocklen_per_process[data->n],
                             (data->max_disp_index[data->n])*sizeof(int));
                        data->displs_per_process[data->n] = (MPI_Aint *) realloc
                            ((void *)data->displs_per_process[data->n],
                             (data->max_disp_index[data->n])*sizeof(MPI_Aint));
                    }

                    data->blocklen_per_process[data->n][data->disp_index[data->n]] = 0;
                    data->displs_per_process[data->n][data->disp_index[data->n]] = 0;
                }

                if (data->procs_in_group[data->n] == rank) {
                    bytes_recvd += data->bytes_remaining;
                }
                data->current_index ++;
                data->bytes_to_read_in_cycle -= data->bytes_remaining;
                data->bytes_remaining = 0;
            } else {
                /* the remaining data from the previous cycle is larger than the
                   data->bytes_to_read_in_cycle, so we have to segment again */
                if (aggregator == rank) {
                    data->blocklen_per_process[data->n][data->disp_index[data->n]] = data->bytes_to_read_in_cycle;
                    data->displs_per_process[data->n][data->disp_index[data->n]] =
                        (ptrdiff_t)data->global_iov_array[data->sorted[data->current_index]].iov_base +
                        (data->global_iov_array[data->sorted[data->current_index]].iov_len
                         - data->bytes_remaining);
                    data->disp_index[data->n] += 1;
                }

                if (data->procs_in_group[data->n] == rank) {
                    bytes_recvd += data->bytes_to_read_in_cycle;
                }
                data->bytes_remaining -= data->bytes_to_read_in_cycle;
                data->bytes_to_read_in_cycle = 0;
                break;
            }
        } else {
            /* No partially used entry available, have to start a new one */
            if (data->bytes_to_read_in_cycle <
                (MPI_Aint) data->global_iov_array[data->sorted[data->current_index]].iov_len) {
                /* This entry has more data than we can sendin one cycle */
                if (aggregator == rank) {
                    data->blocklen_per_process[data->n][data->disp_index[data->n]] = data->bytes_to_read_in_cycle;
                    data->displs_per_process[data->n][data->disp_index[data->n]] =
                        (ptrdiff_t)data->global_iov_array[data->sorted[data->current_index]].iov_base ;
                    data->disp_index[data->n] += 1;
                }
                if (data->procs_in_group[data->n] == rank) {
                    bytes_recvd += data->bytes_to_read_in_cycle;
                }
                data->bytes_remaining = data->global_iov_array[data->sorted[data->current_index]].iov_len -
                    data->bytes_to_read_in_cycle;
                data->bytes_to_read_in_cycle = 0;
                break;
            } else {
                /* Next data entry is less than data->bytes_to_read_in_cycle */
                if (aggregator == rank) {
                    data->blocklen_per_process[data->n][data->disp_index[data->n]] =
                        data->global_iov_array[data->sorted[data->current_index]].iov_len;
                    data->displs_per_process[data->n][data->disp_index[data->n]] = (ptrdiff_t)
                        data->global_iov_array[data->sorted[data->current_index]].iov_base;

                    data->disp_index[data->n] += 1;

                    /*realloc for next blocklength
                      and assign this displacement and check for next displs as
                      the total length of this entry has been consumed!*/
                    if (data->disp_index[data->n] == data->max_disp_index[data->n]) {
                        data->max_disp_index[data->n] *=2 ;
                        data->blocklen_per_process[data->n] = (int *) realloc (
			    (void *)data->blocklen_per_process[data->n],
			    (data->max_disp_index[data->n]*sizeof(int)));
                        data->displs_per_process[data->n] = (MPI_Aint *)realloc (
  			    (void *)data->displs_per_process[data->n],
                            (data->max_disp_index[data->n]*sizeof(MPI_Aint)));
                    }
                    data->blocklen_per_process[data->n][data->disp_index[data->n]] = 0;
                    data->displs_per_process[data->n][data->disp_index[data->n]] = 0;
                }

                if (data->procs_in_group[data->n] == rank) {
                    bytes_recvd += data->global_iov_array[data->sorted[data->current_index]].iov_len;
                }
                data->bytes_to_read_in_cycle -=
                    data->global_iov_array[data->sorted[data->current_index]].iov_len;
                data->current_index ++;
            }
        }
    } /* while (data->bytes_to_read_in_cycle) */

    /*************************************************************************
     *** 7d. Calculate the displacement
     *************************************************************************/
    entries_per_aggregator = 0;
    for (i = 0; i < data->procs_per_group; i++){
	for (j = 0; j < data->disp_index[i]; j++){
	    if (data->blocklen_per_process[i][j] > 0)
		entries_per_aggregator++ ;
	}
    }
    
#if DEBUG_ON
    printf("%d: cycle: %d, bytes_recvd: %d\n ", rank, index, bytes_recvd);
    printf("%d : Entries per aggregator : %d\n", rank, entries_per_aggregator);
#endif

    if (entries_per_aggregator > 0) {
	file_offsets_for_agg = (mca_io_ompio_local_io_array *)
	    malloc (entries_per_aggregator*sizeof(mca_io_ompio_local_io_array));
	if (NULL == file_offsets_for_agg) {
	    opal_output (1, "OUT OF MEMORY\n");
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}

	sorted_file_offsets = (int *) malloc (entries_per_aggregator*sizeof(int));
	if (NULL == sorted_file_offsets){
	    opal_output (1, "OUT OF MEMORY\n");
	    ret =  OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}

	/*Moving file offsets to an IO array!*/
	temp_index = 0;

	for (i = 0; i < data->procs_per_group; i++){
	    for (j = 0; j < data->disp_index[i]; j++){
		if (data->blocklen_per_process[i][j] > 0){
		    file_offsets_for_agg[temp_index].length = data->blocklen_per_process[i][j];
		    file_offsets_for_agg[temp_index].process_id = i;
		    file_offsets_for_agg[temp_index].offset = data->displs_per_process[i][j];
		    temp_index++;

		    //#if DEBUG_ON
		    printf("************Cycle: %d,  Aggregator: %d ***************\n",
			   index+1, rank); 
		    printf("%d sends blocklen[%d]: %d, disp[%d]: %ld to %d\n",
			   rank, j, data->blocklen_per_process[i][j], j,
			   data->displs_per_process[i][j],
			   data->procs_in_group[i]);
		    //#endif
		}
	    }
	}

	/* Sort the displacements for each aggregator*/
	local_heap_sort (file_offsets_for_agg,
			 entries_per_aggregator,
			 sorted_file_offsets);

	/*create contiguous memory displacements
	  based on blocklens on the same displs array
	  and map it to this aggregator's actual
	  file-displacements (this is in the io-array created above)*/
	memory_displacements = (MPI_Aint *) malloc (entries_per_aggregator * sizeof(MPI_Aint));

	memory_displacements[sorted_file_offsets[0]] = 0;
	for (i = 1; i < entries_per_aggregator; i++){
	    memory_displacements[sorted_file_offsets[i]] =
		memory_displacements[sorted_file_offsets[i-1]] +
		file_offsets_for_agg[sorted_file_offsets[i-1]].length;
	}
    } // EDGAR : CHECK

    /**********************************************************
     *** 7f. Create the io array, and pass it to fbtl
     *********************************************************/
    if (entries_per_aggregator > 0) {
        fh->f_io_array = (mca_common_ompio_io_array_t *) malloc
            (entries_per_aggregator * sizeof (mca_common_ompio_io_array_t));
        if (NULL == fh->f_io_array) {
            opal_output(1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }

        fh->f_num_of_io_entries = 0;
        /*First entry for every aggregator*/
        fh->f_io_array[0].offset =
            (IOVBASE_TYPE *)(intptr_t)file_offsets_for_agg[sorted_file_offsets[0]].offset;
        fh->f_io_array[0].length =
            file_offsets_for_agg[sorted_file_offsets[0]].length;
        fh->f_io_array[0].memory_address =
            data->global_buf+memory_displacements[sorted_file_offsets[0]];
        fh->f_num_of_io_entries++;

        for (i = 1; i < entries_per_aggregator; i++){
            /* If the entries are contiguous merge them,
               else make a new entry */
            if (file_offsets_for_agg[sorted_file_offsets[i-1]].offset +
                file_offsets_for_agg[sorted_file_offsets[i-1]].length ==
                file_offsets_for_agg[sorted_file_offsets[i]].offset){
                fh->f_io_array[fh->f_num_of_io_entries - 1].length +=
                    file_offsets_for_agg[sorted_file_offsets[i]].length;
            } else {
                fh->f_io_array[fh->f_num_of_io_entries].offset =
                    (IOVBASE_TYPE *)(intptr_t)file_offsets_for_agg[sorted_file_offsets[i]].offset;
                fh->f_io_array[fh->f_num_of_io_entries].length =
                    file_offsets_for_agg[sorted_file_offsets[i]].length;
                fh->f_io_array[fh->f_num_of_io_entries].memory_address =
                    data->global_buf+memory_displacements[sorted_file_offsets[i]];
                fh->f_num_of_io_entries++;
            }
        }

	//#if DEBUG_ON
        printf("*************************** %d\n", fh->f_num_of_io_entries);
        for (i = 0 ; i < fh->f_num_of_io_entries ; i++) {
            printf(" ADDRESS: %p  OFFSET: %ld   LENGTH: %ld\n",
                   fh->f_io_array[i].memory_address,
                   (ptrdiff_t)fh->f_io_array[i].offset,
                   fh->f_io_array[i].length);
        }
	//#endif
    }

    mca_common_ompio_request_alloc (&ompio_req, MCA_OMPIO_REQUEST_READ);

    if (fh->f_num_of_io_entries) {

        if (1 == read_syncType) {
            if (is_accelerator_buffer) {
                ret = mca_common_ompio_file_iread_pregen(fh, (ompi_request_t *) ompio_req);
                if (0 > ret) {
                    opal_output (1, "vulcan_read_all: mca_common_ompio_iread_pregen failed\n");
                    ompio_req->req_ompi.req_status.MPI_ERROR = ret;
                    ompio_req->req_ompi.req_status._ucount = 0;
                }
            } else {
		printf("%d before ipreadv num_of_io_entries %d address %p offset %lu size %lu \n", rank, fh->f_num_of_io_entries,
		       (void*)fh->f_io_array[0].memory_address, (uint64_t)fh->f_io_array[0].offset, fh->f_io_array[0].length);
                ret = fh->f_fbtl->fbtl_ipreadv(fh, (ompi_request_t *) ompio_req);
                if (0 > ret) {
                    opal_output (1, "vulcan_read_all: fbtl_ipreadv failed\n");
                    ompio_req->req_ompi.req_status.MPI_ERROR = ret;
                    ompio_req->req_ompi.req_status._ucount = 0;
                }
            }
        }
        else {
	    printf("%d before preadv num_of_io_entries %d address %p offset %lu size %lu\n", rank, fh->f_num_of_io_entries,
		   fh->f_io_array[0].memory_address, (uint64_t)fh->f_io_array[0].offset, fh->f_io_array[0].length);
            ret_temp = fh->f_fbtl->fbtl_preadv(fh);
            if (0 > ret_temp) {
                opal_output (1, "vulcan_read_all: fbtl_preadv failed\n");
                ret = ret_temp;
                ret_temp = 0;
            }

            ompio_req->req_ompi.req_status.MPI_ERROR = ret;
            ompio_req->req_ompi.req_status._ucount = ret_temp;
            ompi_request_complete (&ompio_req->req_ompi, false);
        }

        free(fh->f_io_array);
    }
    else {
        ompio_req->req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;
        ompio_req->req_ompi.req_status._ucount = 0;
        ompi_request_complete (&ompio_req->req_ompi, false);
    }

    /*Now update the displacements array with memory offsets*/
#if DEBUG_ON
    global_count = 0;
#endif
    temp_disp_index = (int *)calloc (1, data->procs_per_group * sizeof (int));
    if (NULL == temp_disp_index) {
	opal_output (1, "OUT OF MEMORY\n");
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    for (i = 0; i < entries_per_aggregator; i++){
	temp_pindex =
	    file_offsets_for_agg[sorted_file_offsets[i]].process_id;
	data->displs_per_process[temp_pindex][temp_disp_index[temp_pindex]] =
	    memory_displacements[sorted_file_offsets[i]];
	if (temp_disp_index[temp_pindex] < data->disp_index[temp_pindex]) {
	    temp_disp_index[temp_pindex] += 1;
	} else {
	    printf("temp_disp_index[%d]: %d is greater than disp_index[%d]: %d\n",
		   temp_pindex, temp_disp_index[temp_pindex],
		   temp_pindex, data->disp_index[temp_pindex]);
	}
#if DEBUG_ON
	global_count +=
	    file_offsets_for_agg[sorted_file_offsets[i]].length;
#endif
    }

    if (NULL != temp_disp_index) {
	free(temp_disp_index);
	temp_disp_index = NULL;
    }

#if DEBUG_ON
    printf("************Cycle: %d,  Aggregator: %d ***************\n",
	   index, rank);
    for (i = 0; i < data->procs_per_group; i++) {
	for (j = 0; j < data->disp_index[i]; j++) {
	    if (data->blocklen_per_process[i][j] > 0) {
		printf("%d sends blocklen[%d]: %d, disp[%d]: %ld to %d\n",
		       data->procs_in_group[i],j,
		       data->blocklen_per_process[i][j],j,
		       data->displs_per_process[i][j],
		       rank);
	    }
	}
    }
    printf("************Cycle: %d,  Aggregator: %d ***************\n",
	   index+1, rank);
    for (i = 0; i < entries_per_aggregator; i++) {
	printf("%d: OFFSET: %lld   LENGTH: %ld, Mem-offset: %ld\n",
	       file_offsets_for_agg[sorted_file_offsets[i]].process_id,
	       file_offsets_for_agg[sorted_file_offsets[i]].offset,
	       file_offsets_for_agg[sorted_file_offsets[i]].length,
	       memory_displacements[sorted_file_offsets[i]]);
    }
    printf("%d : global_count : %ld, bytes_recvd : %d\n",
	   rank, global_count, bytes_recvd);
#endif

exit:
    free(sorted_file_offsets);
    free(file_offsets_for_agg);
    free(memory_displacements);
    free(blocklength_proc);
    free(displs_proc);

    fh->f_io_array=NULL;
    fh->f_num_of_io_entries=0;

    *request = (ompi_request_t *) ompio_req;
    return ret;
}

static int shuffle_init (int index, int cycles, int aggregator, int rank, mca_io_ompio_aggregator_data *data,
                         ompi_request_t **reqs)
{
    int i, ret;
#if DEBUG_ON
    MPI_Aint global_count = 0;
#endif
    int* blocklength_proc=NULL;
    ptrdiff_t* displs_proc=NULL;
    int bytes_recvd = 24;
    /*************************************************************************
     *** 7e. Perform the actual communication
     *************************************************************************/
    if (aggregator == rank ) {
	for (i = 0; i < data->procs_per_group; i++) {
	    size_t datatype_size;
	    reqs[i] = MPI_REQUEST_NULL;
	    if (0 < data->disp_index[i]) {
		ompi_datatype_create_hindexed (data->disp_index[i],
					       data->blocklen_per_process[i],
					       data->displs_per_process[i],
					       MPI_BYTE,
					       &data->sendtype[i]);
		ompi_datatype_commit (&data->sendtype[i]);
		opal_datatype_type_size (&data->sendtype[i]->super, &datatype_size);

		if (datatype_size){
		    printf("%d isend to %d disp %ld len %lu\n", rank,  data->procs_in_group[i], data->displs_per_process[i][0], datatype_size);
		    ret = MCA_PML_CALL(isend(data->global_buf,
					     1, data->sendtype[i],
					     data->procs_in_group[i],
					     FCOLL_VULCAN_SHUFFLE_TAG+index,
					     MCA_PML_BASE_SEND_STANDARD,
					     data->comm, &reqs[i]));
		    if (OMPI_SUCCESS != ret){
			goto exit;
		    }
		}
	    }
	}
	// }  /* end if (entries_per_aggr > 0 ) */
    }/* end if (aggregator == rank ) */

    if (bytes_recvd) {
        size_t remaining      = bytes_recvd;
        int block_index       = -1;
        int blocklength_size  = INIT_LEN;

        ptrdiff_t recv_mem_address  = 0;
        ompi_datatype_t *newType    = MPI_DATATYPE_NULL;
        blocklength_proc            = (int *)       calloc (blocklength_size, sizeof (int));
        displs_proc                 = (ptrdiff_t *) calloc (blocklength_size, sizeof (ptrdiff_t));

        if (NULL == blocklength_proc || NULL == displs_proc ) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }

        while (remaining) {
            block_index++;

            if(0 == block_index) {
                recv_mem_address = (ptrdiff_t) (data->decoded_iov[data->iov_index].iov_base) +
                                                data->current_position;
            }
            else {
                // Reallocate more memory if blocklength_size is not enough
                if(0 == block_index % INIT_LEN) {
                    blocklength_size += INIT_LEN;
                    blocklength_proc = (int *)       realloc(blocklength_proc, blocklength_size * sizeof(int));
                    displs_proc      = (ptrdiff_t *) realloc(displs_proc, blocklength_size * sizeof(ptrdiff_t));
                }
                displs_proc[block_index] = (ptrdiff_t) (data->decoded_iov[data->iov_index].iov_base) +
                                                        data->current_position - recv_mem_address;
            }

            if (remaining >=
                (data->decoded_iov[data->iov_index].iov_len - data->current_position)) {

                blocklength_proc[block_index] = data->decoded_iov[data->iov_index].iov_len -
                                                data->current_position;
                remaining = remaining -
                            (data->decoded_iov[data->iov_index].iov_len - data->current_position);
                data->iov_index = data->iov_index + 1;
                data->current_position = 0;
            } else {
                blocklength_proc[block_index] = remaining;
                data->current_position += remaining;
                remaining = 0;
            }
        }

        data->total_bytes_read += data->bytes_recvd;
	data->bytes_recvd = bytes_recvd;

        if (0 <= block_index) {
            ompi_datatype_create_hindexed (block_index+1,
                                           blocklength_proc,
                                           displs_proc,
                                           MPI_BYTE,
                                           &newType);
            ompi_datatype_commit (&newType);

	    printf("%d irecv from %d buf %p len %d\n", rank,  aggregator, (void*)recv_mem_address, blocklength_proc[0]);
            ret = MCA_PML_CALL(irecv((char *)recv_mem_address,
                                     1,
                                     newType,
                                     aggregator,
                                     FCOLL_VULCAN_SHUFFLE_TAG+index,
                                     data->comm,
                                     &reqs[data->procs_per_group]));
            if (MPI_DATATYPE_NULL != newType) {
                ompi_datatype_destroy(&newType);
            }
            if (OMPI_SUCCESS != ret){
                goto exit;
            }
        }
    }

#if DEBUG_ON
    if (aggregator == rank) {
        printf("************Cycle: %d,  Aggregator: %d ***************\n",
               index+1, rank);
        for (i = 0; i < global_count/4; i++)
            printf (" SEND %d \n", ((int *)data->global_buf)[i]);
    }
#endif


exit:

    return OMPI_SUCCESS;
}
