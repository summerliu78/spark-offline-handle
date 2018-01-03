//package com.yinker.tinyv.pagerank.scc;
//
//import org.apache.parquet.it.unimi.dsi.fastutil.Stack;
//
///**
// * Created by t
// * hink on 2017/11/14.
// */
//public class KosarajuSCC {
//    private Digraph G;
//    private Digraph reverseG; //反向图
//    private Stack<Integer> reversePost; //逆后续排列保存在这
//    private boolean[] marked;
//    private int[] id; //第v个点在几个强连通分量中
//    private int count; //强连通分量的数量
//
//    public Kosaraju(Digraph G) {
//        int temp;
//        this.G = G;
//        reverseG = G.reverse();
//        marked = new boolean[G.V()];
//        id = new int[G.V()];
//        reversePost = new Stack<Integer>();
//
//        makeReverPost(); //算出逆后续排列
//
//        for (int i = 0; i < marked.length; i++) { //重置标记
//            marked[i] = false;
//        }
//
//        for (int i = 0; i < G.V(); i++) { //算出强连通分量
//            temp = reversePost.pop();
//            if (!marked[temp]) {
//                count++;
//                dfs(temp);
//            }
//        }
//    }
//
//    /*
//     * 下面两个函数是为了算出 逆后序排列
//     */
//    private void makeReverPost() {
//        for (int i = 0; i < G.V(); i++) { //V()返回的是图G的节点数
//            if (!marked[i])
//                redfs(i);
//        }
//    }
//
//    private void redfs(int v) {
//        marked[v] = true;
//        for (Integer w : reverseG.adj(v)) { //adj(v)返回的是v指向的结点的集合
//            if (!marked[w])
//                redfs(w);
//        }
//        reversePost.push(v); //在这里把v加入栈,完了到时候再弹出来,弹出来的就是逆后续排列
//    }
//
//    /*
//     * 标准的深度优先搜索
//     */
//    private void dfs(int v) {
//        marked[v] = true;
//        id[v] = count;
//        for (Integer w : G.adj(v)) {
//            if (!marked[w])
//                dfs(w);
//        }
//    }
//
//    public int count() {
//        return count;
//    }
//}
