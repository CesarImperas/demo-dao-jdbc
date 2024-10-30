package application;

import java.util.Date;

import model.dao.DaoFactory;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class Program {

	public static void main(String[] args) {
		
		Department dp = new Department(1, "Books");
		
		Seller seller = new Seller(21, "Bob", "bob@gmail.com", new Date(), 3000.0, dp);
		
		
		// Dessa forma, o programa não conhece a implementação, somente a interface
		SellerDao sellerDao = DaoFactory.createrSellerDao();
		
		System.out.println(seller);
	}

}
