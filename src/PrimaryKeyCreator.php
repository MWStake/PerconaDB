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
	 * @return void
	 */
	public function execute() {
		if ( $this->hasTablesWithoutPrimaryKeys() ) {
			$this->addPrimaryKeys();
		}
	}

	/**
	 * Add primary keys to tables that need them.
	 * @return void
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
	 * @return void
	 */
	protected function addPrimaryKey( $table ) {
		$sql = $this->getSQLToAddPrimaryKey( $table );
		if ( $sql === false ) {
			throw new Exception( "Could not create a primary key for the $table.\n" );
		}
		$this->dbw->query( $sql );
	}

	/**
	 * Remove the prefix from the given table name
	 *
	 * @param string $table
	 * @return string
	 */
	protected function removePrefix( $table ) {
		global $wgDBprefix;
		$len = strlen( $wgDBprefix );

		if ( substr( $table, 0, $len ) === $wgDBprefix ) {
			$table = substr_replace( $table, '', 0, $len );
		}
		return $table;
	}

	/**
	 * Get the SQL to add a primary key.
	 *
	 * @param string $table
	 * @return false|string
	 */
	protected function getSQLToAddPrimaryKey( $table ) {
		$npTable = $this->removePrefix( $table );
		$sql = false;

		$sqlMap = [
			'oldimage' => '( oi_sha1, oi_timestamp )',
			'querycache' => '( qc_namespace, qc_title )',
			'querycachetwo' => [
				'ALTER TABLE querycachetwo '
				. 'ADD COLUMN qcc_id int(10) UNSIGNED NOT NULL '
				. 'AUTO_INCREMENT PRIMARY KEY'
			],
			'user_newtalk' => '( user_id, user_ip, user_last_timestamp )',
			'smw_di_blob' => '(p_id,s_id,o_hash)',
			'smw_di_bool' => '(p_id,s_id,o_value)',
			'smw_di_uri' => '(p_id,s_id,o_serialized)',
			'smw_di_coords' => '(p_id,s_id,o_serialized)',
			'smw_di_wikipage' => '(p_id,s_id,o_id)',
			'smw_di_number' => '(p_id,s_id,o_serialized)',
			'smw_fpt_ask' => '(s_id,o_id)',
			'smw_fpt_askde' => '(s_id,o_serialized)',
			'smw_fpt_askfo' => '(s_id,o_hash)',
			'smw_fpt_askdu' => '(s_id,o_serialized)',
			'smw_fpt_asksi' => '(s_id,o_serialized)',
			'smw_fpt_askst' => '(s_id,o_hash)',
			'smw_fpt_cdat' => '(s_id,o_serialized)',
			'smw_fpt_conc' => '(s_id)',
			'smw_fpt_conv' => '(s_id,o_hash)',
			'smw_fpt_dtitle' => '(s_id,o_hash)',
			'smw_fpt_impo' => '(s_id,o_hash)',
			'smw_fpt_inst' => '(s_id,o_id)',
			'smw_fpt_lcode' => '(s_id,o_hash)',
			'smw_fpt_ledt' => '(s_id,o_id)',
			'smw_fpt_list' => '(s_id,o_hash)',
			'smw_fpt_mdat' => '(s_id,o_serialized)',
			'smw_fpt_media' => '(s_id,o_hash)',
			'smw_fpt_mime' => '(s_id,o_hash)',
			'smw_fpt_newp' => '(s_id,o_value)',
			'smw_fpt_pplb' => '(s_id,o_id)',
			'smw_fpt_prec' => '(s_id,o_serialized)',
			'smw_fpt_pval' => '(s_id,o_hash)',
			'smw_fpt_redi' => '(s_title, s_namespace)',
			'smw_fpt_serv' => '(s_id,o_hash)',
			'smw_fpt_sobj' => '(s_id,o_id)',
			'smw_fpt_subc' => '(s_id,o_id)',
			'smw_fpt_subp' => '(s_id,o_id)',
			'smw_fpt_text' => '(s_id,o_hash)',
			'smw_fpt_type' => '(s_id,o_serialized)',
			'smw_fpt_unit' => '(s_id,o_hash)',
			'smw_fpt_uri' => '(s_id,o_serialized)',
			'smw_prop_stats' => '(p_id)',
			'smw_query_links' => '(s_id,o_id)',
			'smw_concept_cache' => '(s_id,o_id)',
			'smw_fpt_askpa' => '(s_id,o_hash)',
		];

		if ( isset( $sqlMap[$npTable] ) ) {
			$replace = $sqlMap[$npTable];
			if ( !is_array( $replace ) ) {
				$sql = sprintf(
					"ALTER TABLE %s ADD PRIMARY KEY %s",
					$this->dbw->addIdentifierQuotes( $table ), $replace
				);
			} else {
				$sql = array_shift( $replace );
			}
			if ( $sql ) {
				$sql .= " COMMENT 'added by PrimaryKeyCreator'";
			}
		}
		return $sql;
	}
}
