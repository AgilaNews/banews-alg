#include <iostream>
#include <vector>
#include <algorithm>
#include <errno.h>
#include <cstring>
#include "linear.h"
#include "eval.h"

#define Malloc(type,n) (type *)malloc((n)*sizeof(type))

typedef std::vector<double> dvec_t;
typedef std::vector<int>    ivec_t;

// prototypes of evaluation functions
double precision(const dvec_t& dec_values, const ivec_t& ty);
double recall(const dvec_t& dec_values, const ivec_t& ty);
double fscore(const dvec_t& dec_values, const ivec_t& ty);
double bac(const dvec_t& dec_values, const ivec_t& ty);
double auc(const dvec_t& dec_values, const ivec_t& ty);
double accuracy(const dvec_t& dec_values, const ivec_t& ty);

// evaluation function pointer
// You can assign this pointer to any above prototype
double (*validation_function)(const dvec_t&, const ivec_t&) = auc;
//double (*validation_function)(const dvec_t&, const ivec_t&) = accuracy;


static char *line = NULL;
static int max_line_len;


static char* readline(FILE *input)
{
	int len;
	
	if(fgets(line,max_line_len,input) == NULL)
		return NULL;

	while(strrchr(line,'\n') == NULL)
	{
		max_line_len *= 2;
		line = (char *) realloc(line,max_line_len);
		len = (int) strlen(line);
		if(fgets(line+len,max_line_len-len,input) == NULL)
			break;
	}
	return line;
}



double precision(const dvec_t& dec_values, const ivec_t& ty){
	size_t size = dec_values.size();
	size_t i;
	int    tp, fp;
	double precision;

	tp = fp = 0;

	for(i = 0; i < size; ++i) if(dec_values[i] >= 0){
		if(ty[i] == 1) ++tp;
		else           ++fp;
	}

	if(tp + fp == 0){
		fprintf(stderr, "warning: No postive predict label.\n");
		precision = 0;
	}else
		precision = tp / (double) (tp + fp);
	printf("Precision = %g%% (%d/%d)\n", 100.0 * precision, tp, tp + fp);
	
	return precision;
}



double recall(const dvec_t& dec_values, const ivec_t& ty){
	size_t size = dec_values.size();
	size_t i;
	int    tp, fn; // true_positive and false_negative
	double recall;

	tp = fn = 0;

	for(i = 0; i < size; ++i) if(ty[i] == 1){ // true label is 1
		if(dec_values[i] >= 0) ++tp; // predict label is 1
		else                   ++fn; // predict label is -1
	}

	if(tp + fn == 0){
		fprintf(stderr, "warning: No postive true label.\n");
		recall = 0;
	}else
		recall = tp / (double) (tp + fn);
	// print result in case of invocation in prediction
	printf("Recall = %g%% (%d/%d)\n", 100.0 * recall, tp, tp + fn);
	
	return recall; // return the evaluation value
}



double fscore(const dvec_t& dec_values, const ivec_t& ty){
	size_t size = dec_values.size();
	size_t i;
	int    tp, fp, fn;
	double precision, recall;
	double fscore;

	tp = fp = fn = 0;

	for(i = 0; i < size; ++i) 
		if(dec_values[i] >= 0 && ty[i] == 1) ++tp;
		else if(dec_values[i] >= 0 && ty[i] == -1) ++fp;
		else if(dec_values[i] <  0 && ty[i] == 1) ++fn;

	if(tp + fp == 0){
		fprintf(stderr, "warning: No postive predict label.\n");
		precision = 0;
	}else
		precision = tp / (double) (tp + fp);
	if(tp + fn == 0){
		fprintf(stderr, "warning: No postive true label.\n");
		recall = 0;
	}else
		recall = tp / (double) (tp + fn);

	
	if(precision + recall == 0){
		fprintf(stderr, "warning: precision + recall = 0.\n");
		fscore = 0;
	}else
		fscore = 2 * precision * recall / (precision + recall);

	printf("F-score = %g\n", fscore);
	
	return fscore;
}



double bac(const dvec_t& dec_values, const ivec_t& ty){
	size_t size = dec_values.size();
	size_t i;
	int    tp, fp, fn, tn;
	double specificity, recall;
	double bac;

	tp = fp = fn = tn = 0;

	for(i = 0; i < size; ++i) 
		if(dec_values[i] >= 0 && ty[i] == 1) ++tp;
		else if(dec_values[i] >= 0 && ty[i] == -1) ++fp;
		else if(dec_values[i] <  0 && ty[i] == 1)  ++fn;
		else ++tn;

	if(tn + fp == 0){
		fprintf(stderr, "warning: No negative true label.\n");
		specificity = 0;
	}else
		specificity = tn / (double)(tn + fp);
	if(tp + fn == 0){
		fprintf(stderr, "warning: No positive true label.\n");
		recall = 0;
	}else
		recall = tp / (double)(tp + fn);

	bac = (specificity + recall) / 2;
	printf("BAC = %g\n", bac);
	
	return bac;
}



// only for auc
class Comp{
	const double *dec_val;
	public:
	Comp(const double *ptr): dec_val(ptr){}
	bool operator()(int i, int j) const{
		return dec_val[i] > dec_val[j];
	}
};


double auc(const dvec_t& dec_values, const ivec_t& ty){
	double roc  = 0;
	size_t size = dec_values.size();
	size_t i;
	std::vector<size_t> indices(size);

	for(i = 0; i < size; ++i) indices[i] = i;

	std::sort(indices.begin(), indices.end(), Comp(&dec_values[0]));

	int tp = 0,fp = 0;
	for(i = 0; i < size; i++) {
		if(ty[indices[i]] == 1) tp++;
		else if(ty[indices[i]] == -1) {
			roc += tp;
			fp++;
		}
	}

	if(tp == 0 || fp == 0)
	{
		fprintf(stderr, "warning: Too few postive true labels or negative true labels\n");
		roc = 0;
	}
	else
		roc = roc / tp / fp;

	printf("AUC = %g\n", roc);

	return roc;
}



double accuracy(const dvec_t& dec_values, const ivec_t& ty){
	int    correct = 0;
	int    total   = (int) ty.size();
	size_t i;

	for(i = 0; i < ty.size(); ++i)
		if(ty[i] == (dec_values[i] >= 0? 1: -1)) ++correct;

	printf("Accuracy = %g%% (%d/%d)\n",
		(double)correct/total*100,correct,total);

	return (double) correct / total;
}



double binary_class_cross_validation(const problem *prob, const parameter *param, int nr_fold)
{
	int i;
	int *fold_start = Malloc(int,nr_fold+1);
	int l = prob->l;
	int *perm = Malloc(int,l);
	int *labels;
	dvec_t dec_values;
	ivec_t ty;

	for(i=0;i<l;i++) perm[i]=i;
	for(i=0;i<l;i++)
	{
		int j = i+rand()%(l-i);
		std::swap(perm[i],perm[j]);
	}
	for(i=0;i<=nr_fold;i++)
		fold_start[i]=i*l/nr_fold;

	for(i=0;i<nr_fold;i++)
	{
		int                begin   = fold_start[i];
		int                end     = fold_start[i+1];
		int                j,k;
		struct problem subprob;

		subprob.n = prob->n;
		subprob.bias = prob->bias;
		subprob.l = l-(end-begin);
		subprob.x = Malloc(struct feature_node*,subprob.l);
		subprob.y = Malloc(double,subprob.l);

		k=0;
		for(j=0;j<begin;j++)
		{
			subprob.x[k] = prob->x[perm[j]];
			subprob.y[k] = prob->y[perm[j]];
			++k;
		}
		for(j=end;j<l;j++)
		{
			subprob.x[k] = prob->x[perm[j]];
			subprob.y[k] = prob->y[perm[j]];
			++k;
		}
		struct model *submodel = train(&subprob,param);

	
		

		labels = Malloc(int, get_nr_class(submodel));
		get_labels(submodel, labels);

		if(get_nr_class(submodel) > 2) 
		{
			fprintf(stderr,"Error: the number of class is not equal to 2\n");
			exit(-1);
		}

		dec_values.resize(end);
		ty.resize(end);

		for(j=begin;j<end;j++) {
			predict_values(submodel,prob->x[perm[j]], &dec_values[j]);
			ty[j] = (prob->y[perm[j]] > 0)? 1: -1;
		}


		if(labels[0] <= 0) {
			for(j=begin;j<end;j++)
				dec_values[j] *= -1;
		}
	
		free_and_destroy_model(&submodel);
		free(subprob.x);
		free(subprob.y);
		free(labels);
	}		

	free(perm);
	free(fold_start);

	return validation_function(dec_values, ty);	
}


void binary_class_predict(FILE *input, FILE *output){
	int    total = 0;
	int    *labels;
	int    max_nr_attr = 64;
	struct feature_node *x = Malloc(struct feature_node, max_nr_attr);
	dvec_t dec_values;
	ivec_t true_labels;
	int n;
	if(model_->bias >= 1)
		n = get_nr_feature(model_) + 1;	
	else
		n = get_nr_feature(model_);


	labels = Malloc(int, get_nr_class(model_));
	get_labels(model_, labels);
	
	max_line_len = 1024;
	line = (char *)malloc(max_line_len*sizeof(char));
	while(readline(input) != NULL)
	{
		int i = 0;
		double target_label, predict_label;
		char *idx, *val, *label, *endptr;
		int inst_max_index = -1; // strtol gives 0 if wrong format, and precomputed kernel has <index> start from 0

		label = strtok(line," \t");
		target_label = strtod(label,&endptr);
		if(endptr == label)
			exit_input_error(total+1);

		while(1)
		{
			if(i>=max_nr_attr - 2)	// need one more for index = -1
			{
				max_nr_attr *= 2;
				x = (struct feature_node *) realloc(x,max_nr_attr*sizeof(struct feature_node));
			}

			idx = strtok(NULL,":");
			val = strtok(NULL," \t");

			if(val == NULL)
				break;
			errno = 0;
			x[i].index = (int) strtol(idx,&endptr,10);
			if(endptr == idx || errno != 0 || *endptr != '\0' || x[i].index <= inst_max_index)
				exit_input_error(total+1);
			else
				inst_max_index = x[i].index;

			errno = 0;
			x[i].value = strtod(val,&endptr);
			if(endptr == val || errno != 0 || (*endptr != '\0' && !isspace(*endptr)))
				exit_input_error(total+1);

			++i;
		}
		
		if(model_->bias >= 0){
			x[i].index = n;
			x[i].value = model_->bias; 
			++i;	
		}
	
		x[i].index = -1;

		predict_label = predict(model_,x);
		fprintf(output,"%g\n",predict_label);


		double dec_value;
		predict_values(model_, x, &dec_value);
		true_labels.push_back((target_label > 0)? 1: -1);
		if(labels[0] <= 0) dec_value *= -1;
		dec_values.push_back(dec_value);
	}	

	validation_function(dec_values, true_labels);

	free(labels);
	free(x);
}
