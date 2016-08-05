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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/info/info.h"
#include "ompi/mca/io/base/base.h"
#include "common_ompio.h"
#include "ompi/file/file.h"
#include "ompi/mca/io/ompio/io_ompio.h"
#include "io_ompio_logging.h"

ompi_file_t *ompio_log_file;
mca_io_ompio_file_t *ompio_log_fh=NULL;
double ompio_log_base_time=0;


static void io_ompio_log_init (ompi_communicator_t *comm,  const char *filename );
static void io_ompio_log_finalize (void );
#define LOG_VERBOSE 1

void io_ompio_log_init (ompi_communicator_t *comm,
                        const char *filename )
{
    if ( 0 == ompio_log_base_time ) {
        ompio_log_base_time= MPI_Wtime();      
        ompi_file_open ( comm, filename,
                         MPI_MODE_WRONLY | MPI_MODE_CREATE,
                         MPI_INFO_NULL, &ompio_log_file );

        mca_io_ompio_data_t *data= (mca_io_ompio_data_t *) ompio_log_file->f_io_selected_data;
        ompio_log_fh = &data->ompio_fh;

    }
    return; 
}


void io_ompio_log_finalize ()
{
    // Close the file
    ompi_file_close ( &ompio_log_file);
    ompio_log_base_time = 0;
    return;
}

void io_ompio_log ( mca_io_ompio_file_t *ompio_file, int event_type, ...)
{
    va_list args;
    char buf[IO_OMPIO_LOG_MAX_BUF_LEN];
    static char bufopen[IO_OMPIO_LOG_MAX_BUF_LEN];
    static char bufinit[IO_OMPIO_LOG_MAX_BUF_LEN];
    int size, fstype;
    long frompos, topos;
    int operation, i;
    int pos=0;
    static int openpos=0, initpos=0;
    static int counter=0;
    static int protect=0;
    
    if ( protect > 0 ) {
        return;
    }
    protect++;

    if ( counter == 0 ) {
        io_ompio_log_init ( ompio_file->f_comm, "logfile.out" );
    }
    mca_sharedfp_base_module_t *shared_fp_base_module= (ompio_log_fh)->f_sharedfp;;
    int id=(ompio_log_fh)->f_rank;
    double current_time = MPI_Wtime() - ompio_log_base_time;
    
    if ( counter == 2 ) {
        if ( id == 0 ) 
            shared_fp_base_module->sharedfp_write(ompio_log_fh,bufopen,openpos, MPI_CHAR, MPI_STATUS_IGNORE );    
        shared_fp_base_module->sharedfp_write(ompio_log_fh,bufinit,initpos, MPI_CHAR, MPI_STATUS_IGNORE );    
    }

    va_start (args, event_type );

    switch (event_type) {
        case IO_OMPIO_LOG_EVENT_OPEN :            

            size = ompio_file->f_size;
            fstype = ompio_file->f_fstype;
            memset ( bufopen, 0, IO_OMPIO_LOG_MAX_BUF_LEN );
            snprintf ( bufopen, IO_OMPIO_LOG_MAX_BUF_LEN, "%d;%s;%d;%d;", event_type, ompio_file->f_filename,fstype,size); 
            openpos = 15+IO_OMPIO_LOG_MAX_FILENAME_LEN;
#ifdef LOG_VERBOSE
            if ( id == 0 )  printf("%s\n", bufopen);
#endif
            counter++;
//           shared_fp_base_module->sharedfp_write(&ompio_log_fh,buf,pos, MPI_CHAR, MPI_STATUS_IGNORE );
            break;
        case IO_OMPIO_LOG_EVENT_FVIEW_SET:
            
            memset ( buf, 0, IO_OMPIO_LOG_MAX_BUF_LEN );

            snprintf ( buf, 25, "%d;%d;%lf;%d;", event_type,id,current_time,(int)ompio_file->f_iov_count+1 );
            pos += 25;
            for ( i=0; i<(int)ompio_file->f_iov_count; i++ ) {
                snprintf(&buf[pos], 18, "%ld;%ld;", (long)ompio_file->f_decoded_iov[i].iov_base, ompio_file->f_decoded_iov[i].iov_len );
                pos += 18;
                if ( pos > IO_OMPIO_LOG_MAX_BUF_LEN ) {
                    printf("Buffer exceeds maximum size. Please increase  IO_OMPIO_LOG_MAX_BUF_LEN and recompile ompio\n");
                    MPI_Abort (MPI_COMM_WORLD, 1 );
                    return;
                }
            }
            snprintf(&buf[pos], 18, "%ld;%ld;", (long)ompio_file->f_disp+ompio_file->f_view_extent, (long)0 );

#ifdef LOG_VERBOSE
                printf("%s", buf);
                int k=25;
                for ( i=0; i<(int)ompio_file->f_iov_count; i++ ) {
                    printf("%s", &buf[k]);
                    k+=18;
                }
                printf("%s\n", &buf[k]);
#endif


            if ( counter == 1 ) {
                /* this is the call from set_file_defaults, the sharedfp component is not yet
                   set up. */
                memcpy ( bufinit, buf, pos );
                initpos = pos;
            }
            else {
                shared_fp_base_module->sharedfp_write(ompio_log_fh,buf,pos, MPI_CHAR, MPI_STATUS_IGNORE );
            }
            counter++;    
            break;
        case IO_OMPIO_LOG_EVENT_UPDATE:
            
            frompos   = va_arg (args, long );
            topos     = va_arg (args, long );
            operation = va_arg (args, int );

            memset ( buf, 0, IO_OMPIO_LOG_MAX_BUF_LEN );
            snprintf( buf, IO_OMPIO_LOG_MAX_BUF_LEN, "%d;%d;%lf;%ld;%ld;%d", event_type,id,current_time,frompos,topos,operation ); 
            pos = strlen(buf)+1;

#ifdef LOG_VERBOSE
            printf("%s\n", buf);
#endif
            shared_fp_base_module->sharedfp_write(ompio_log_fh,buf,pos, MPI_CHAR, MPI_STATUS_IGNORE );
            counter++;    
            break;
        case IO_OMPIO_LOG_EVENT_CLOSE:
            io_ompio_log_finalize ();
            break;
        default:
            break;
    }

    protect--;

    va_end (args);
}


