
/* Algorithm: Use K-D Tree to speed searching. */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <ctime>
#include <cmath>
#include <queue>
#include <cassert>
#include <algorithm>
#include <string>

#define sqr(x) ((x)*(x))

using namespace std;

const int max_dimension = 55;

/* K, number of dimension */
int dimension;

/* K dimension point */
struct point {

	static int cmp_dimension; /* The dimension used to compare two points */

	double coor[max_dimension]; /* point coordinates */

  string label; /* label (or meaning) of this point */

	bool operator < (const point &b) const {
		return coor[cmp_dimension] < b.coor[cmp_dimension];
	}

	bool operator != (const point &b) const {
		for (int i=0; i<dimension; i++)
			if (coor[i]!=b.coor[i])
				return 1;
		return 0;
	}
};

int point::cmp_dimension = 0;

/* Data points in the input */
vector <point> data_points;

/* K-D Tree class */
template <typename DataType, class CalcDist>
class KDTree {

	/* Node class */
	struct kd_node {
		int dir; /* The dimension used to compare */
		DataType data; /* The point data in the node */
		kd_node *ngtv, *pstv; /* Negative and Positive */

		kd_node(int D=0)
			: dir(D), ngtv(NULL), pstv(NULL) {}

	};

	kd_node *root; /* Root of the tree */

	/* Build K-D Tree according to certain point sequence */
	struct kd_node * build_node(vector <DataType> &points) {
		if (points.empty())
			return NULL;
		kd_node *root;

		/* If there is only one point */
		if (points.size() == 1) {
			root = new kd_node();
			root->data = *points.begin();
		}
		/* Multiple points */
		else {
			root = new kd_node(DataType::cmp_dimension = rand() % dimension);

			/* Split the vector according to the (size/2)th element */
			std::nth_element(points.begin(), points.begin() + points.size() / 2, points.end());
			root->data = points[points.size() / 2];
			vector <DataType> vec_ng(points.begin(), points.begin() + points.size() / 2),
				   vec_ps(points.begin() + points.size() / 2 + 1, points.end());

			/* Build recursively */
			root->ngtv = build_node(vec_ng);
			root->pstv = build_node(vec_ps);
		}

		return root;
	}

	/* Free memory */
	void erase(kd_node *root){
    if (!root) return;
		if (root->ngtv)
			erase(root->ngtv);
		if (root->pstv)
			erase(root->pstv);
		delete root;
	}

	/* Pair of distance and candidate point */
	struct pair_type {
		int dist; /* Distance */
		DataType data; /* The candidate point */

		pair_type(int d=0, DataType *p=0): dist(d) {
			if (p)
				data = *p;
		}

		bool operator < (const pair_type &b) const {
			return dist < b.dist;
		}
	};

	/* Candidate set with size M */
	priority_queue < pair_type > que;

	/* Query in the subtree whose root is 'root' */
	void query(kd_node *root, const DataType &query_point, int m){
		kd_node *n = root->ngtv, *p = root->pstv, *r = root;
		bool flag=0;

		pair_type cur(CalcDist()(query_point, r->data), &r->data);
		DataType::cmp_dimension = r->dir;

		if (! (query_point < r->data))
			swap(n, p);

		/* Check in the closer subtree */
		if (n)
			query(n, query_point, m);

		/* Candidate set is not full */
		if (que.size() < m){
			que.push(cur);
			flag=1;
		}
		/* Check whether replace someone in the candidate set */
		else {
			if (cur.dist < que.top().dist){
				que.pop();
				que.push(cur);
			}
			if (sqr(query_point.coor[r->dir] - r->data.coor[r->dir]) < que.top().dist)
				flag = 1;
		}

		/* Check in the farther subtree */
		if (p && flag)
			query(p, query_point, m);
	}

	public:
		KDTree(){
			srand(time(0));
		}

		void construct(vector <DataType> &points) {
			root = build_node(points);
		}

		~KDTree(){
			erase(root);
		}

		void query(const DataType &query_point, int m){
      printf("%s", query_point.label.c_str());

			for (; !que.empty(); que.pop());
			query(root, query_point, m);
			vector <DataType> seq;
			while (!que.empty()){
				seq.push_back(que.top().data);
				que.pop();
			}
			for (int i=m-1; i>=0; i--){
				printf(" %s", seq[i].label.c_str());
			}
      printf("\n");
		}

};

class CalcDist{

	public:
	int operator ()(point a, point b) {
		int re(0);
		for (int i=0; i<dimension; i++)
			re += (a.coor[i] - b.coor[i]) * (a.coor[i] - b.coor[i]);
		return re;
	}
};

const int STRLEN = 2560;
char st[STRLEN];

void read_data_points() {
  FILE *in = fopen("../similarResumeData/58feature_resumevectors_text", "r");
  int line = 0;
  for (; fgets(st, STRLEN, in) && !feof(in); ) {
    line++;
    fprintf(stderr, "# reading line %d.\r", line);
    point new_point;
    char tmp[64], *p, *q;
    memset(tmp, 0, sizeof(tmp));
    for (p = st + 1, q = tmp; *p != ','; p++, q++) *q = *p;
    new_point.label = string(tmp);
    p = p + strlen(",WrappedArray(");
    dimension = 0;
    while (*p != ')') {
      while (*p == ' ' || *p == ')' || *p == ',') p++;
      for (q=p; *q!=',' && *q!=')'; q++);
      int len = q-p;
      memcpy(tmp, p, len);
      tmp[len] = 0;
      sscanf(tmp, "%lf", new_point.coor+dimension);
      dimension++;
      p = q;
    }
    data_points.push_back(new_point);
  }
  fclose(in);
  fprintf(stderr, "\nread finish (%d points).\n",
      data_points.size());
}

void normalize_data_points() {
  for (vector <point> :: iterator it=data_points.begin();
      it != data_points.end(); it++) {
    double sum = 0;
    for (int i=0; i<dimension; i++)
      sum += sqr(it->coor[i]);
    sum = sqrt(sum);
    for (int i=0; i<dimension; i++)
      it->coor[i] /= sum;
  }
  fprintf(stderr, "normalization finish.\n");
}

int main(){

  int M = 100;
	point query_point;
	srand(time(0));

  freopen("output.log", "w", stdout);

  read_data_points();
  normalize_data_points();

	KDTree <struct point, CalcDist> kdtree;

	kdtree.construct(data_points);

  int cnt = 0;
  for (vector <point> :: iterator it=data_points.begin();
      it != data_points.end(); it++) {
    cnt++;
    fprintf(stderr, "# processing node %d.\r", cnt);
    query_point = *it;
    kdtree.query(query_point, M);
  }
  fprintf(stderr, "done\n");

  fclose(stdout);

	return 0;
}

