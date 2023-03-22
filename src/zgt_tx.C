/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Spring 2023: CSE 4331/5331 Project 2 : Tx Manager */

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>


extern void *start_operation(long, long);  //start an op with mutex lock and cond wait
extern void *finish_operation(long);        //finish an op with mutex unlock and con signal

extern void *do_commit_abort(long, char);   //commit/abort based on char value
extern void *process_read_write(long, long, int, char);

extern zgt_tm *ZGT_Sh;			// Transaction manager object

FILE *logfile = NULL;

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

zgt_tx::zgt_tx( long tid, char Txstatus, char type, pthread_t thrid){
  this->lockmode = (char)' ';   // default
  this->Txtype = type;          // R = read only, W=Read/Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1;              // set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1;             // init to an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is not empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list return NULL
   }
  return(NULL);				// if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //intialise a transaction object. Make sure it is 
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. When creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
  
  struct param *node = (struct param*)arg;// get tid and count
  start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node

    // Write to logfile  that transaction has started
    open_logfile();
    fprintf(logfile, "T%d\t%c \tBeginTx\n", node->tid, node->Txtype);
    fflush(logfile);
  
    zgt_p(0);				// Lock Tx manager; Add node to transaction list
  
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;   
    zgt_v(0); 			// Release tx manager 
  fprintf(ZGT_Sh->logfile, "T%d\t%c \tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
    fflush(ZGT_Sh->logfile);
  finish_operation(node->tid);
  pthread_exit(NULL);				// thread exit
}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg){
  struct param *node = (struct param*)arg;// get tid and objno and count

  start_operation(node->tid,node->count);
  zgt_p(0);
  open_logfile();
	zgt_tx *tx = get_tx(node->tid);
	if (tx != NULL)
	{
		if(tx->status == TR_ABORT || tx->status == TR_END)
		{
			do_commit_abort(tx->tid, tx->status);
			zgt_v(0);
			finish_operation(tx->tid);
			pthread_exit(NULL);
		}
    else if(tx->status==TR_ACTIVE)
    {
    tx->set_lock(node->tid, 1, node->obno, node->count, 'S');
    zgt_v(0);
    finish_operation(tx->tid);
    pthread_exit(NULL); 
    }
    else if(tx->status == TR_WAIT)
    {
      do_commit_abort(tx->tid, TR_WAIT);
      fprintf(logfile, "T%d\t%c\tWaiting\n", node->tid, node->Txtype);
      fflush(logfile);
      zgt_v(0);
      finish_operation(tx->tid);
      pthread_exit(NULL);
    }
	} else {
    fprintf(logfile, "Transaction with id %d doesn't exist", node->tid);
  }

}


void *writetx(void *arg){ //do the operations for writing; similar to readTx
  struct param *node = (struct param*)arg;	// struct parameter that contains
  
  start_operation(node->tid,node->count);
  zgt_p(0);
  open_logfile();
	zgt_tx *tx = get_tx(node->tid);
	if (tx != NULL)
	{
		if(tx->status == TR_ABORT || tx->status == TR_END)
		{
			do_commit_abort(tx->tid, tx->status);
			zgt_v(0);
			finish_operation(tx->tid);
			pthread_exit(NULL);
		}
    else if(tx->status==TR_ACTIVE)
    {
    tx->set_lock(node->tid, 1, node->obno, node->count, 'X');
    zgt_v(0);
    finish_operation(tx->tid);
    pthread_exit(NULL); 
    }
    else if(tx->status == TR_WAIT)
    {
      do_commit_abort(tx->tid, TR_WAIT);
      fprintf(logfile, "T%d\t%c\tWaiting\n", node->tid, node->Txtype);
      fflush(logfile);
      zgt_v(0);
      finish_operation(tx->tid);
      pthread_exit(NULL);
    }
	} else {
    fprintf(logfile, "Transaction with id %d doesn't exist", node->tid);
  }

}

// common method to process read/write: Just a suggestion

void *process_read_write(long tid, long obno,  int count, char mode){

  
}

void *aborttx(void *arg)
{
  struct param *node = (struct param*)arg;// get tid and count  

  start_operation(node->tid, node->count); 
  zgt_p(0);
  do_commit_abort(node->tid, TR_ABORT);
  zgt_v(0);
  finish_operation(node->tid);

  pthread_exit(NULL);			// thread exit
}

void *committx(void *arg)
{
 
    //remove the locks/objects before committing
  struct param *node = (struct param*)arg;// get tid and count

  start_operation(node->tid, node->count); 
  zgt_p(0);
  do_commit_abort(node->tid, TR_END);
  zgt_v(0);
  finish_operation(node->tid);
  pthread_exit(NULL);			// thread exit
}

//suggestion as they are very similar

// called from commit/abort with appropriate parameter to do the actual
// operation. Make sure you give error messages if you are trying to
// commit/abort a non-existent tx

void *do_commit_abort(long t, char status){
  open_logfile();
	if(status == TR_ABORT){ // abort if the status is to abort
	  fprintf(logfile, "T%d\tAbortTx \t",t);
    fflush(logfile);
  }
	else { // commit the transaction
    fprintf(logfile, "T%d\tCommmitTx \t",t);
    fflush(logfile);
  }
  // get the transaction
	zgt_tx *tx = get_tx(t);
	
	if (tx != NULL) {
    // free the locks of all transactions
    if(tx->status == TR_WAIT){
      fflush(stdout);
      fprintf(logfile, "NOTE: T%d\t wrong state \n", t);
      fflush(logfile);
    }
    tx->status = status;
    tx->free_locks();
    int transac_no = tx->semno;
    tx->remove_tx();
    if (transac_no != -1) {
      int waitingTransactions = zgt_nwait(transac_no);
      for(int i = 1; i <= waitingTransactions; i++)
        zgt_v(transac_no); // release the lock held by the transaction
      }
	} else {
		fprintf(logfile, "\n No transaction found\n");
		fflush(logfile);
		fflush(stdout);
	}
}

int zgt_tx::remove_tx ()
{
  //remove the transaction from the TM
  
  zgt_tx *txptr, *lastr1;
  lastr1 = ZGT_Sh->lastr;
  for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){	// scan through list
	  if (txptr->tid == this->tid){		// if correct node is found          
		 lastr1->nextr = txptr->nextr;	// update nextr value; done
		 //delete this;
         return(0);
	  }
	  else lastr1 = txptr->nextr;			// else update prev value
   }
  fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(ZGT_Sh->logfile);
  printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(stdout);
  return(-1);
}

/* this method sets lock on objno1 with lockmode1 for a tx*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1){
  //if the thread has to wait, block the thread on a semaphore from the
  //sempool in the transaction manager. Set the appropriate parameters in the
  //transaction list if waiting.
  //if successful  return(0); else -1
  int lock = 0;

  while (lock == 0) {
    // gets the lock of the tx-manager
    zgt_p(0);
    // gets the transaction 
    zgt_tx *tx = get_tx(tid1);

    // search for the object in the hash table
    zgt_hlink *node = ZGT_Ht->find(sgno1, obno1);

    // check if object is already in the hashtable, NULL means not on the hash table
    if (node == NULL) {
      // add the object to the hashtable and get the needed lock (lockmode)
      ZGT_Ht->add(tx, tx->sgno, obno1, tx->lockmode);
      // TR_ACTIVE status makes it active
      tx->status = TR_ACTIVE;

      // release the lock
      zgt_v(0);

      tx->perform_readWrite(tid, obno, lockmode); // perform readwrite
    } else if (node->tid == tid1) { // else if the obj is held by the same transaction  
      // simply change the transaction status to active and release the lock
      tx->status = TR_ACTIVE;
      zgt_v(0); 
      tx->perform_readWrite(tid, obno, lockmode); // perform readwrite
      return 0;   
    } else { // the lock is held by different transaction
      // get the transaction that is holding the object
      zgt_tx  *holding_tx = get_tx(node->tid);
      // get other transactions that is trying to get the lock
      zgt_hlink *waiting_txs = others_lock(node, sgno1, obno1);

      if (tx->Txtype == 'R' && holding_tx->Txtype == 'R') {
        lock = 1;

        if (tx->head == NULL) {
          ZGT_Ht->add(tx, tx->sgno, obno1, tx->lockmode);
          node = ZGT_Ht->find(tx-> sgno, obno1);
          tx->head = node;
          tx->status = TR_ACTIVE;
        } else {
          //iterate to the end of object list to add new required object node at the end
          zgt_hlink *h = tx->head;
          while(h->nextp != NULL){
            h = h->nextp;
          }
          h->nextp = node; 
          tx->status = TR_ACTIVE;
          tx->perform_readWrite(tid, obno, lockmode); // perform readwrite
        }
      } else {
        tx->status = TR_WAIT;
        tx->obno = obno1;
        tx->lockmode = lockmode1;
        if (get_tx(node->tid)) {
          tx->setTx_semno(node->tid, node->tid); //Set semaphore on current tx
        }
        else {
          tx->status = TR_ACTIVE;
        }

        tx->print_tm();
        zgt_v(0);
        zgt_p(node->tid); //Hold txn with the object.
        lock = false;
      }
      zgt_v(0); // release the lock held by transaction manager
      return 0
    }
  }
}

int zgt_tx::free_locks()
{
  
  // this part frees all locks owned by the transaction
  // that is, remove the objects from the hash table
  // and release all Tx's waiting on this Tx

  zgt_hlink* temp = head;  //first obj of tx
  
  for(temp;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

      fprintf(ZGT_Sh->logfile, "%d : %d, ", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
      fflush(ZGT_Sh->logfile);
      
      if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
	   printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
	   fflush(stdout);
      }
      else {
#ifdef TX_DEBUG
	   printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n",
                            temp->tid, temp->obno, temp->lockmode);
	   fflush(stdout);
#endif
      }
    }
  fprintf(ZGT_Sh->logfile, "\n");
  fflush(ZGT_Sh->logfile);
  
  return(0);
}		

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  
{
  zgt_tx *linktx, *prevp;
  
  // USED to COMMIT 
  //remove the transaction and free all associate dobjects. For the time being 
  //this can be used for commit of the transaction.
  
  linktx = prevp = ZGT_Sh->lastr;
  
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
}

// currently not used
int zgt_tx::cleanup()
{
  return(0);
  
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print
void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx  list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

// check all transactions holding the lock for the objno and return the hash_node
zgt_hlink *zgt_tx::others_lock(zgt_hlink *hnodep, long sgno1, long obno1)
{
  zgt_hlink *node;
  node = ZGT_Ht->find(sgno1,obno1); // find the object in the hashtable and return teh first node with the objno
  while (node != NULL) {
    if (node->obno == obno1 && node->sgno == sgno1 && node->tid != this->tid) {
      return (node); // return the hashnode that holds the lock
    }
    else {
      node = node->next; // otherwise, look to the next node waiting for the obj
    }
  }
  return (NULL); // return null if no transaction is found to hold the lock
}
//need to be called for printing
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}

void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}

// routine to perform the acutual read/write operation
// based  on the lockmode

void zgt_tx::perform_readWrite(long tid,long obno, char lockmode){
  
  zgt_p(0);        // Lock Tx manager;
  // get the transaction
  zgt_tx *txptr=get_tx(tid);
  int i;
  open_logfile();
  if(lockmode == 'S'){  //Read only mode
    //decrement the object value by 1 for read
    ZGT_Sh->objarray[obno]->value -= 1;

    //write to log record and flush the file buffer stream
    fprintf(logfile, "T%d\t\tReadTx\t\t%d:%d:%d\t\tReadLock\tGranted\t%c\n",tid,obno,ZGT_Sh->objarray[obno]->value,ZGT_Sh->optime[tid],txptr->status);
    fflush(logfile);
  }
  else if(lockmode == 'X') { 
    // increment the object value by 1 for write
    ZGT_Sh->objarray[obno]->value += 1;

    //write to log record and flush the file buffer stream
    fprintf(logfile, "T%d\t\tWriteTx\t %d:%d:%d\t\t WriteLock\tGranted\t%c\n",tid,obno,ZGT_Sh->objarray[obno]->value,ZGT_Sh->optime[tid],txptr->status);
    fflush(logfile);
  }
  else {
    printf("Invalid lock mode\n");
  }
  // release the lock of the transaction manager
  zgt_v(0);
}

// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%d which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    exit(1);
  }
  if ((txptr->semno == -1)|| (txptr->semno == semno)){  //just to be safe
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%d\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
  
}

// Otherside of teh start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
}

/**
 * @brief open the logfile
 * 
 * @return void* 
 */
void *open_logfile() {
  if((logfile = fopen(ZGT_Sh->logfile, "a")) == NULL) {
    printf("\nCouldn't open the log file:%s\n", ZGT_Sh->logfile);
    fflush(stdout); // flush the stdout stream if any logs had not been flushed
    exit(1); // terminates the calling process
  }
}
