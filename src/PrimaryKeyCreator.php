<?php #-*-tab-width: 4; fill-column: 76; indent-tabs-mode: t -*-
# vi:shiftwidth=4 tabstop=4 textwidth=76

/**
 * Copyright (C) 2019  NicheWork, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author Mark A. Hershberger <mah@nichework.com>
 */
namespace MediaWiki\Extension\PerconaDB;

use DatabaseUpdater;
use Database;

class PrimaryKeyCreator {
	protected $dbw;
	protected $upd;
	protected $dbName;
	protected $quotedDBName;
	protected $idQuotedDBName;
	protected $tableMap;

	/**
	 * @param DatabaseUpdater $upd
	 */
	public function __construct( DatabaseUpdater $upd ) {
		$this->upd = $upd;
		$this->dbw = $upd->getDB();
		$this->dbName = $this->dbw->getDBName();
		$this->quotedDBName = $this->dbw->addQuotes( $this->dbName );
		$this->idQuotedDBName = $this->dbw->addIdentifierQuotes( $this->dbName );
		$this->tableMap = [];
	}

	/**
	 * Take care of everything that needs to be done to give all the tables a primary key.
	 */
	public function execute() {
		if ( $this->hasTablesWithoutPrimaryKeys() ) {
			$this->addPrimaryKeys();
		}
	}

	/**
	 * Add primary keys to tables that need them.
	 */
	protected function addPrimaryKeys() {
		$this->upd->output( "Updating tables to have a primary key...\n" );
		foreach ( array_keys( $this->tableMap ) as $table ) {
			$this->upd->output( "...$table\n" );
			$this->addPrimaryKey( $table );
		}
	}

	/**
	 * Store an array of tables without a primary key and the type of key
	 * (UNI, MUL) used for each column.  This information is used later to
	 * construct a primary key. Returns true if any are found.
	 *
	 * @param Database $dbw
	 * @return bool
	 */
	protected function hasTablesWithoutPrimaryKeys() {
		$res = $this->dbw->query(
			"SELECT DISTINCT table_name, column_key, column_name
						FROM information_schema.columns c1
					   WHERE column_key IN ('MUL', 'UNI')
						 AND table_schema={$this->quotedDBName}
						 AND NOT EXISTS (SELECT *
										   FROM information_schema.columns
										  WHERE c1.table_name = table_name
											AND column_key = 'PRI'
											AND table_schema={$this->quotedDBName})"
		);
		if ( $res->numRows() ) {
			foreach ( $res as $row ) {
				$this->tableMap[$row->table_name][$row->column_key][] = $row->column_name;
			}
		}
		return count( $this->tableMap ) > 0;
	}

	/**
	 * Add a primary key to the table that doesn't currently have one.
	 *
	 * @param string $table
	 */
	protected function addPrimaryKey( $table ) {
		$sql = $this->getSQLToAddPrimaryKey( $table );
		if ( $sql === false ) {
			throw new Exception( "Could not create a primary key for the $table.\n" );
		}
		$this->dbw->query( $sql );
	}

	/**
	 * Get the SQL to add a primary key.
	 *
	 * @param string $table
	 */
	protected function getSQLToAddPrimaryKey( $table ) {
		$sql = false;

		$sqlMap = [
			'oldimage' => '( oi_sha1, oi_timestamp )',
			'querycache' => '( qc_namespace, qc_title )',
			'querycachetwo' => [
				'ALTER TABLE querycachetwo '
				. 'ADD COLUMN qcc_id int(10) UNSIGNED NOT NULL '
				. 'AUTO_INCREMENT PRIMARY KEY'
			],
			'user_newtalk' => '( user_id, user_ip, user_last_timestamp )'
		];
		if ( isset( $sqlMap[$table] ) && !is_array( $sqlMap[$table] ) ) {
			$sql = sprintf(
				"ALTER TABLE %s ADD PRIMARY KEY %s",
				$this->dbw->addIdentifierQuotes( $table ), $sqlMap[$table]
			);
		} elseif ( isset( $sqlMap[$table] ) && is_array( $sqlMap[$table] ) ) {
			$sql = array_shift( $sqlMap[$table] );
		}
		if ( $sql ) {
			$sql .= " COMMENT 'added by PrimaryKeyCreator'";
		}
		return $sql;
	}
}
