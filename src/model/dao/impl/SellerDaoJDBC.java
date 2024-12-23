package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {

	private Connection conn;
	
	// Dependência para a inicialização do atributo "conn"
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void insert(Seller obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("INSERT INTO seller " +
										"(Name, Email, BirthDate, BaseSalary, DepartmentId) " +
										"VALUES " + 
										"(?, ?, ?, ?, ?)",
										Statement.RETURN_GENERATED_KEYS);
			
			// Preenchimento dos valores para cada placeholders
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4,  obj.getBaseSalary());
			st.setInt(5,  obj.getDepartment().getId());
			
			int rowsAffected = st.executeUpdate();
			
			if(rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				if(rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			} else {
				throw new DbException("Unexpected error! No rows affected!");
			}
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void update(Seller obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("UPDATE seller " +
										"SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? " +
										"WHERE Id = ?", Statement.RETURN_GENERATED_KEYS);
			
			// Preenchimento dos valores para cada placeholders
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4,  obj.getBaseSalary());
			st.setInt(5,  obj.getDepartment().getId());
			
			st.setInt(6, obj.getId());
			
			st.executeUpdate();
		
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("DELETE FROM seller " +
										"WHERE Id = ?"
										);
			
			st.setInt(1, id);
			
			int rows = st.executeUpdate();
			
			if(rows == 0) {
				throw new DbException("Id does not exist in DBA!");
			}
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
					"SELECT seller.*, department.Name as DepName " +
					"FROM seller INNER JOIN department " +
					"ON seller.DepartmentId = department.Id " +
					"WHERE seller.Id = ?"
					);
			// Substituindo o placeholder com o id do parâmetro
			st.setInt(1, id);
			
			// Execução da consulta do Statement, retornando um ResultSet
			rs = st.executeQuery();
			
			// Verificar se existe algum dado na consulta (se retornar valor 0 - false, não possui)
			if(rs.next()) {
				// Criando e preenchendo os atributos do objeto Department (igual ao UML disposto no resumo)
				Department dep = instantiateDepartment(rs);
				
				// Criando e preenchendo os atributos do objeto Seller
				Seller seller = instantiateSeller(rs, dep);
				
				return seller;
			}
			
			return null;
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
			
			// CUIDADO! Não fechamos a conexão com o banco de dados, por ser uma classe que 
			// possui outros métodos de acesso a ele
		}
	}

	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
		Seller seller = new Seller();
		seller.setId(rs.getInt("Id"));
		seller.setName(rs.getString("Name"));
		seller.setEmail(rs.getString("Email"));
		seller.setBaseSalary(rs.getDouble("BaseSalary"));
		seller.setBirthDate(rs.getDate("BirthDate"));
		
		// Estabelecendo a associação entre os objetos Department e Seller
		seller.setDepartment(dep);
		
		return seller;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("DepartmentId"));
		dep.setName(rs.getString("DepName"));
		
		return dep;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
					"SELECT seller.*, department.Name as DepName " +
					"FROM seller INNER JOIN department " +
					"ON seller.DepartmentId = department.Id " +
					"ORDER BY Name"
					);
	
			// Execução da consulta do Statement, retornando um ResultSet
			rs = st.executeQuery();
			
			List<Seller> list = new ArrayList<Seller>();
			Map<Integer, Department> map = new HashMap<Integer, Department>();
			
			// Verificar se existe algum dado na consulta (se retornar valor 0 - false, não possui)
			while(rs.next()) {
				
				// Buscar um departamento do dicionário, a partir do Id de um dado do ResultSet
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				// Departamento não existe (cria uma nova instância)
				if(dep == null) {
					dep = instantiateDepartment(rs);
					
					// Adicionar o novo departamento no dicionário
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				// Criando e preenchendo os atributos do objeto Seller
				Seller seller = instantiateSeller(rs, dep);
				
				list.add(seller);
			}
			
			return list;
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
					"SELECT seller.*, department.Name as DepName " +
					"FROM seller INNER JOIN department " +
					"ON seller.DepartmentId = department.Id " +
					"WHERE DepartmentId = ? " +
					"ORDER BY Name"
					);
			// Substituindo o placeholder com o id do parâmetro
			st.setInt(1, department.getId());
			
			// Execução da consulta do Statement, retornando um ResultSet
			rs = st.executeQuery();
			
			List<Seller> list = new ArrayList<Seller>();
			Map<Integer, Department> map = new HashMap<Integer, Department>();
			
			// Verificar se existe algum dado na consulta (se retornar valor 0 - false, não possui)
			while(rs.next()) {
				
				// Buscar um departamento do dicionário, a partir do Id de um dado do ResultSet
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				// Departamento não existe (cria uma nova instância)
				if(dep == null) {
					dep = instantiateDepartment(rs);
					
					// Adicionar o novo departamento no dicionário
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				// Criando e preenchendo os atributos do objeto Seller
				Seller seller = instantiateSeller(rs, dep);
				
				list.add(seller);
			}
			
			return list;
			
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

}
