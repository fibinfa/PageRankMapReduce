package MR.HW3;

import java.util.List;


/**
 * Node class that stores information of each page after converting
 * the data set to a graph structure
 * @author fibinfa
 *
 */
public class Node {
	private String pageName;
	private List<String> links;
	private double pageRank;
	
	/**
	 * Constructor
	 * @param pageName
	 * @param links
	 */
	public Node(String pageName, List<String> links) {
		this.pageName = pageName;
		this.links = links;
	}
	public Node() {
		
	}
	public String getName() {
		return pageName;
	}
	public void setName(String name) {
		this.pageName = name;
	}
	public List<String> getLinks() {
		return links;
	}
	public void setLinks(List<String> links) {
		this.links = links;
	}
	public double getPageRank() {
		return pageRank;
	}
	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}
	
	
	/*
	 * overriden the to string method to print the adjacency list
	 */
	@Override
	public String toString() {
		if(links.size()>0) {
			StringBuilder sb = new StringBuilder();
			for(String str: links) {
				sb.append(str).append(",");
			}
			return sb.toString().substring(0, sb.toString().length()-1);
		}
		else 
			return "[]";
	}
	
	
	
}
